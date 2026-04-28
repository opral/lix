mod support;

use lix_engine::engine2::ExecuteResult;
use lix_engine::{Engine, Value};

simulation_test2!(engine_new_rejects_uninitialized_backend, |sim| async move {
    match Engine::new(sim.uninitialized_backend()).await {
        Ok(_) => panic!("uninitialized backend should not create an engine"),
        Err(error) => assert_eq!(error.code, "LIX_ERROR_NOT_INITIALIZED"),
    }
});

simulation_test2!(
    engine_initialize_seeds_repository_bootstrap_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_global_session(&engine)
            .await
            .expect("initialized backend should open global session");
        let main_session = sim
            .open_main_session(&engine)
            .await
            .expect("initialized backend should open main session");

        let version_result = session
            .execute(
                "SELECT entity_id, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'lix_version_descriptor' \
             ORDER BY entity_id",
                &[],
            )
            .await
            .expect("version descriptors should be readable");
        let ExecuteResult::Rows(version_rows) = version_result else {
            panic!("SELECT should return version rows");
        };
        assert_eq!(version_rows.len(), 2);
        let version_values = version_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert!(version_values.contains(&vec![
            Value::Text("global".to_string()),
            Value::Text("{\"hidden\":true,\"id\":\"global\",\"name\":\"global\"}".to_string()),
        ]));
        assert!(version_values.contains(&vec![
            Value::Text(sim.main_version_id().to_string()),
            Value::Text(format!(
                "{{\"hidden\":false,\"id\":\"{}\",\"name\":\"main\"}}",
                sim.main_version_id()
            )),
        ]));

        let lix_id_result = session
            .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
            .await
            .expect("lix_id key value should be readable");
        assert_single_text(lix_id_result, &format!("\"{}\"", sim.lix_id()));

        let refs_result = session
            .execute(
                "SELECT entity_id, snapshot_content, untracked \
             FROM lix_state \
             WHERE schema_key = 'lix_version_ref' \
             ORDER BY entity_id",
                &[],
            )
            .await
            .expect("version refs should be readable");
        let ExecuteResult::Rows(ref_rows) = refs_result else {
            panic!("SELECT should return version ref rows");
        };
        assert_eq!(ref_rows.len(), 2);
        let ref_values = ref_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert!(ref_values.contains(&vec![
            Value::Text("global".to_string()),
            Value::Text(format!(
                "{{\"commit_id\":\"{}\",\"id\":\"global\"}}",
                sim.initial_commit_id()
            )),
            Value::Boolean(true),
        ]));
        assert!(ref_values.contains(&vec![
            Value::Text(sim.main_version_id().to_string()),
            Value::Text(format!(
                "{{\"commit_id\":\"{}\",\"id\":\"{}\"}}",
                sim.initial_commit_id(),
                sim.main_version_id()
            )),
            Value::Boolean(true),
        ]));

        drop(main_session);
        drop(session);
        drop(engine);
    }
);

simulation_test2!(
    session_execute_inserts_key_value_then_reads_it_back,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let uuid_result = session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect("session should expose lix_uuid_v7 UDF");
        let ExecuteResult::Rows(uuid_rows) = uuid_result else {
            panic!("SELECT should return uuid rows");
        };
        assert_eq!(uuid_rows.len(), 1);
        let Value::Text(uuid) = &uuid_rows.rows()[0].values()[0] else {
            panic!("lix_uuid_v7 should return text");
        };
        assert!(
            !uuid.is_empty(),
            "lix_uuid_v7 should return a non-empty UUID"
        );

        let insert_result = session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('sql2-key', 'sql2-value')",
                &[],
            )
            .await
            .expect("session insert should succeed");
        assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

        let result = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key = 'sql2-key'",
                &[],
            )
            .await
            .expect("session read should succeed");
        let ExecuteResult::Rows(row_set) = result else {
            panic!("SELECT should return rows");
        };
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("sql2-key".to_string()),
                Value::Text("\"sql2-value\"".to_string()),
            ]
        );
    }
);

simulation_test2!(
    session_execute_persists_deterministic_function_sequence_across_sessions,
    options = support::simulation_test::engine2::Engine2SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open first session");

        let mode_result = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', \
                 lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode insert should succeed");
        assert_eq!(mode_result, ExecuteResult::AffectedRows(1));

        assert_single_text(
            session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("first deterministic uuid should succeed"),
            "01920000-0000-7000-8000-000000000000",
        );
        assert_single_text(
            session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("second deterministic uuid should succeed"),
            "01920000-0000-7000-8000-000000000001",
        );

        let second_session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open second session");
        assert_single_text(
            second_session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("third deterministic uuid should succeed"),
            "01920000-0000-7000-8000-000000000002",
        );
        let write_result = second_session
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
                 ) VALUES (\
                 lix_uuid_v7(), 'lix_key_value', NULL, NULL, lix_json('{\"key\":\"det-write\",\"value\":\"ok\"}'), '1', false, false\
                 )",
                &[],
            )
            .await
            .expect("deterministic write should succeed");
        assert_eq!(write_result, ExecuteResult::AffectedRows(1));
        assert_single_text(
            second_session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("uuid after deterministic write should continue"),
            // The tracked write consumes deterministic values for the
            // SQL-provided entity id, row metadata, and commit metadata.
            "01920000-0000-7000-8000-000000000009",
        );
    }
);

simulation_test2!(
    session_execute_does_not_persist_deterministic_sequence_after_failed_statement,
    options = support::simulation_test::engine2::Engine2SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let mode_result = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', \
                 lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode insert should succeed");
        assert_eq!(mode_result, ExecuteResult::AffectedRows(1));

        let failed_read = session
            .execute("SELECT lix_uuid_v7() FROM missing_engine2_table", &[])
            .await;
        assert!(
            failed_read.is_err(),
            "missing table query should fail before persisting deterministic sequence"
        );
        assert_single_text(
            session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("first deterministic uuid should still start at zero"),
            "01920000-0000-7000-8000-000000000000",
        );

        let failed_write = session
            .execute(
                "INSERT INTO missing_engine2_table VALUES (lix_uuid_v7())",
                &[],
            )
            .await;
        assert!(
            failed_write.is_err(),
            "failed write should not persist deterministic sequence"
        );
        assert_single_text(
            session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("second deterministic uuid should continue after last success"),
            "01920000-0000-7000-8000-000000000001",
        );
    }
);

simulation_test2!(
    session_execute_registers_schema_then_writes_lix_state_row,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let register_schema_result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (\
             lix_json('{\"x-lix-key\":\"engine2_dummy_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
             true,\
             true\
             )",
            &[],
        )
        .await
        .expect("session registered schema insert should succeed");
        assert_eq!(register_schema_result, ExecuteResult::AffectedRows(1));

        let insert_state_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'dummy-1', 'engine2_dummy_schema', NULL, NULL, lix_json('{\"id\":\"dummy-1\",\"name\":\"Dummy\"}'), '1', false, true\
             )",
            &[],
        )
        .await
        .expect("session lix_state insert for registered schema should succeed");
        assert_eq!(insert_state_result, ExecuteResult::AffectedRows(1));

        let result = session
            .execute(
                "SELECT entity_id, schema_key, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'engine2_dummy_schema' AND entity_id = 'dummy-1'",
                &[],
            )
            .await
            .expect("session lix_state read should succeed");
        let ExecuteResult::Rows(row_set) = result else {
            panic!("SELECT should return rows");
        };
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("dummy-1".to_string()),
                Value::Text("engine2_dummy_schema".to_string()),
                Value::Text("{\"id\":\"dummy-1\",\"name\":\"Dummy\"}".to_string()),
            ]
        );
    }
);

simulation_test2!(
    session_execute_inserts_directory_then_reads_it_back,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let insert_result = session
            .execute(
                "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
                &[],
            )
            .await
            .expect("session directory insert should succeed");
        assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

        let nested_insert_result = session
            .execute(
                "INSERT INTO lix_directory (id, path, hidden) \
             VALUES ('dir-nested', '/docs/nested/', false)",
                &[],
            )
            .await
            .expect("session nested directory path insert should succeed");
        assert_eq!(nested_insert_result, ExecuteResult::AffectedRows(1));

        let result = session
            .execute(
                "SELECT id, path, parent_id, name, hidden \
             FROM lix_directory \
             WHERE id IN ('dir-docs', 'dir-nested') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("session directory read should succeed");
        let ExecuteResult::Rows(row_set) = result else {
            panic!("SELECT should return rows");
        };
        assert_eq!(row_set.len(), 2);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("dir-docs".to_string()),
                Value::Text("/docs/".to_string()),
                Value::Null,
                Value::Text("docs".to_string()),
                Value::Boolean(false),
            ]
        );
        assert_eq!(
            row_set.rows()[1].values(),
            &[
                Value::Text("dir-nested".to_string()),
                Value::Text("/docs/nested/".to_string()),
                Value::Text("dir-docs".to_string()),
                Value::Text("nested".to_string()),
                Value::Boolean(false),
            ]
        );
    }
);

simulation_test2!(
    session_execute_inserts_file_then_reads_it_back,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let file_result = session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await
            .expect("session file insert should succeed");
        assert_eq!(file_result, ExecuteResult::AffectedRows(1));

        let result = session
            .execute(
                "SELECT id, path, data, hidden \
             FROM lix_file \
             WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("session file read should succeed");
        let ExecuteResult::Rows(row_set) = result else {
            panic!("SELECT should return rows");
        };
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("file-readme".to_string()),
                Value::Text("/docs/guides/readme.md".to_string()),
                Value::Blob(b"hello".to_vec()),
                Value::Boolean(false),
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
            .expect("session staged filesystem state read should succeed");
        let ExecuteResult::Rows(staged_state_rows) = staged_state_result else {
            panic!("SELECT should return filesystem state rows");
        };
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
            .expect("session directory read after file insert should succeed");
        let ExecuteResult::Rows(directory_rows) = directory_result else {
            panic!("SELECT should return directory rows");
        };
        assert_eq!(
            directory_rows.len(),
            2,
            "file path insert should stage exactly the two missing parent directories"
        );
    }
);

simulation_test2!(
    session_execute_updates_file_path_and_preserves_data,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let insert_result = session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await
            .expect("session file insert should succeed");
        assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

        let update_result = session
            .execute(
                "UPDATE lix_file \
             SET path = '/docs/readme-renamed.md' \
             WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("session file path update should succeed");
        assert_eq!(update_result, ExecuteResult::AffectedRows(1));

        let file_result = session
            .execute(
                "SELECT id, path, data \
             FROM lix_file \
             WHERE id = 'file-readme'",
                &[],
            )
            .await
            .expect("session file read after path update should succeed");
        let ExecuteResult::Rows(file_rows) = file_result else {
            panic!("SELECT should return file rows");
        };
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
            .expect("session filesystem state read after path update should succeed");
        let ExecuteResult::Rows(state_rows) = state_result else {
            panic!("SELECT should return filesystem state rows");
        };
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
            .expect("session directory read after path update should succeed");
        let ExecuteResult::Rows(directory_rows) = directory_result else {
            panic!("SELECT should return directory rows");
        };
        assert_eq!(
            directory_rows.len(),
            2,
            "path update should not stage an extra directory descriptor"
        );
    }
);

simulation_test2!(
    session_execute_deletes_directory_recursively,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim
            .open_main_session(&engine)
            .await
            .expect("backend should open a session");

        let file_result = session
            .execute(
                "INSERT INTO lix_file (id, path, data, hidden) \
             VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                &[],
            )
            .await
            .expect("session file insert should succeed");
        assert_eq!(file_result, ExecuteResult::AffectedRows(1));

        let directory_ids_result = session
            .execute(
                "SELECT id \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("session directory id read before delete should succeed");
        let ExecuteResult::Rows(directory_id_rows) = directory_ids_result else {
            panic!("SELECT should return directory id rows");
        };
        assert_eq!(directory_id_rows.len(), 2);
        let directory_ids = directory_id_rows
            .rows()
            .iter()
            .map(|row| {
                let Value::Text(id) = &row.values()[0] else {
                    panic!("directory id should be text");
                };
                id.clone()
            })
            .collect::<Vec<_>>();

        let delete_result = session
            .execute("DELETE FROM lix_directory WHERE path = '/docs/'", &[])
            .await
            .expect("session recursive directory delete should succeed");
        assert_eq!(delete_result, ExecuteResult::AffectedRows(1));

        let directories_result = session
            .execute(
                "SELECT id, path \
             FROM lix_directory \
             WHERE path IN ('/docs/', '/docs/guides/') \
             ORDER BY path",
                &[],
            )
            .await
            .expect("session directory read after delete should succeed");
        let ExecuteResult::Rows(directory_rows) = directories_result else {
            panic!("SELECT should return directory rows");
        };
        assert_eq!(
            directory_rows.len(),
            0,
            "recursive directory delete should hide the root and child directories"
        );

        let file_result = session
            .execute(
                "SELECT id, path \
             FROM lix_file \
             WHERE path = '/docs/guides/readme.md'",
                &[],
            )
            .await
            .expect("session file read after delete should succeed");
        let ExecuteResult::Rows(file_rows) = file_result else {
            panic!("SELECT should return file rows");
        };
        assert_eq!(
            file_rows.len(),
            0,
            "recursive directory delete should hide nested files"
        );

        let state_result = session
            .execute(
                &format!(
                    "SELECT entity_id, schema_key \
                 FROM lix_state \
                 WHERE entity_id IN ('{}', '{}', 'file-readme') \
                 ORDER BY schema_key, entity_id",
                    directory_ids[0], directory_ids[1]
                ),
                &[],
            )
            .await
            .expect("session state read after delete should succeed");
        let ExecuteResult::Rows(state_rows) = state_result else {
            panic!("SELECT should return state rows");
        };
        assert_eq!(
            state_rows.len(),
            0,
            "recursive directory delete should make descriptor/blob-ref state rows not visible"
        );
    }
);

fn assert_single_text(result: ExecuteResult, expected: &str) {
    let ExecuteResult::Rows(row_set) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(row_set.len(), 1);
    assert_eq!(
        row_set.rows()[0].values(),
        &[Value::Text(expected.to_string())]
    );
}
