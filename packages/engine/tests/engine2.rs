mod support;

mod engine2 {
    pub mod commit_graph;
    pub mod sql;
}

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
            // SQL-provided entity id, row metadata, commit metadata, and the
            // derived change-set id.
            "01920000-0000-7000-8000-00000000000a",
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
