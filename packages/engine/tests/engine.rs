#[path = "support/mod.rs"]
mod support;

use lix_engine::ExecuteResult;
use lix_engine::{CreateVersionOptions, Engine, MergeVersionOptions, SwitchVersionOptions, Value};
use serde_json::json;

simulation_test!(engine_new_rejects_uninitialized_backend, |sim| async move {
    match Engine::new(sim.uninitialized_backend()).await {
        Ok(_) => panic!("uninitialized backend should not create an engine"),
        Err(error) => assert_eq!(error.code, "LIX_ERROR_NOT_INITIALIZED"),
    }
});

simulation_test!(
    engine_initialize_seeds_repository_bootstrap_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("initialized backend should open global session"),
            &engine,
        );
        let main_session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("initialized backend should open main session"),
            &engine,
        );

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
        let version_rows = version_result;
        assert_eq!(version_rows.len(), 2);
        let version_values = version_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert!(version_values.contains(&vec![
            Value::Json(json!(["global"])),
            Value::Json(json!({"hidden": true, "id": "global", "name": "global"})),
        ]));
        assert!(version_values.contains(&vec![
            Value::Json(json!([sim.main_version_id()])),
            Value::Json(json!({"hidden": false, "id": sim.main_version_id(), "name": "main"})),
        ]));

        let lix_id_result = session
            .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
            .await
            .expect("lix_id key value should be readable");
        assert_single_json(lix_id_result, &format!("\"{}\"", sim.lix_id()));

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
        let ref_rows = refs_result;
        assert_eq!(ref_rows.len(), 2);
        let ref_values = ref_rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert!(ref_values.contains(&vec![
            Value::Json(json!(["global"])),
            Value::Json(json!({"commit_id": sim.initial_commit_id(), "id": "global"})),
            Value::Boolean(true),
        ]));
        assert!(ref_values.contains(&vec![
            Value::Json(json!([sim.main_version_id()])),
            Value::Json(json!({"commit_id": sim.initial_commit_id(), "id": sim.main_version_id()})),
            Value::Boolean(true),
        ]));

        drop(main_session);
        drop(session);
        drop(engine);
    }
);

simulation_test!(
    session_execute_inserts_key_value_then_reads_it_back,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("backend should open a session"),
            &engine,
        );

        let uuid_result = session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect("session should expose lix_uuid_v7 UDF");
        let uuid_rows = uuid_result;
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
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));

        let result = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key = 'sql2-key'",
                &[],
            )
            .await
            .expect("session read should succeed");
        let row_set = result;
        assert_eq!(row_set.len(), 1);
        assert_eq!(
            row_set.rows()[0].values(),
            &[
                Value::Text("sql2-key".to_string()),
                Value::Json(json!("sql2-value")),
            ]
        );
    }
);

simulation_test!(
    failed_write_validation_does_not_poison_session_transaction,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("backend should open a session");

        register_poison_task_schema(&session).await;

        let error = session
            .execute(
                "INSERT INTO poison_task (id, title) VALUES ('bad-task', 'missing meta')",
                &[],
            )
            .await
            .expect_err("schema validation should reject missing required field");
        assert_eq!(error.code, "LIX_ERROR_SCHEMA_VALIDATION");

        assert_single_integer(
            session
                .execute("SELECT 1 AS ok", &[])
                .await
                .expect("read after failed write should succeed"),
            1,
        );

        let insert_result = session
            .execute(
                "INSERT INTO poison_task (id, title, meta) \
                 VALUES ('good-task', 'valid', lix_json('{\"priority\":\"high\"}'))",
                &[],
            )
            .await
            .expect("valid write after failed write should succeed");
        assert_eq!(insert_result, ExecuteResult::from_rows_affected(1));
    }
);

simulation_test!(
    session_close_is_idempotent_and_rejects_later_operations,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("backend should open a session");

        session.close().await.expect("first close should succeed");
        session.close().await.expect("second close should succeed");
        assert!(session.is_closed());

        assert_closed(
            session
                .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
                .await
                .expect_err("execute after close should fail"),
        );
        assert_closed(
            session
                .active_version_id()
                .await
                .expect_err("active_version_id after close should fail"),
        );
        assert_closed(
            session
                .create_version(CreateVersionOptions {
                    id: Some("closed-version".to_string()),
                    name: "Closed".to_string(),
                    from_commit_id: None,
                })
                .await
                .expect_err("create_version after close should fail"),
        );
        match session
            .switch_version(SwitchVersionOptions {
                version_id: sim.main_version_id().to_string(),
            })
            .await
        {
            Ok(_) => panic!("switch_version after close should fail"),
            Err(error) => assert_closed(error),
        }
        assert_closed(
            session
                .merge_version(MergeVersionOptions {
                    source_version_id: sim.main_version_id().to_string(),
                })
                .await
                .expect_err("merge_version after close should fail"),
        );
    }
);

async fn register_poison_task_schema(session: &lix_engine::SessionContext) {
    let schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "poison_task",
                "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    });

    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("schema registration should succeed");
}

simulation_test!(
    session_close_state_is_shared_with_switched_session,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("backend should open a session");
        let (switched_session, _) = session
            .switch_version(SwitchVersionOptions {
                version_id: sim.main_version_id().to_string(),
            })
            .await
            .expect("switch_version should succeed before close");

        session.close().await.expect("close should succeed");

        assert_closed(
            switched_session
                .active_version_id()
                .await
                .expect_err("derived session should observe closed state"),
        );
    }
);

simulation_test!(
    session_execute_persists_deterministic_function_sequence_across_sessions,
    options = support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("backend should open first session"),
            &engine,
        );

        let mode_result = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', \
                 lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode insert should succeed");
        assert_eq!(mode_result, ExecuteResult::from_rows_affected(1));

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

        let second_session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("backend should open second session"),
            &engine,
        );
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
					 entity_id, schema_key, file_id, snapshot_content, global, untracked\
					 ) VALUES (\
					 lix_json('[\"det-write\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"det-write\",\"value\":\"ok\"}'), false, false\
					 )",
					&[],
				)
            .await
            .expect("deterministic write should succeed");
        assert_eq!(write_result, ExecuteResult::from_rows_affected(1));
        assert_single_text(
            second_session
                .execute("SELECT lix_uuid_v7()", &[])
                .await
                .expect("uuid after deterministic write should continue"),
            // The tracked write consumes deterministic values for row
            // metadata and commit metadata.
            "01920000-0000-7000-8000-000000000008",
        );
    }
);

simulation_test!(
    session_execute_does_not_persist_deterministic_sequence_after_failed_statement,
    options = support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("backend should open a session"),
            &engine,
        );

        let mode_result = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', \
                 lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode insert should succeed");
        assert_eq!(mode_result, ExecuteResult::from_rows_affected(1));

        let failed_read = session
            .execute("SELECT lix_uuid_v7() FROM missing_engine_table", &[])
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
                "INSERT INTO missing_engine_table VALUES (lix_uuid_v7())",
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
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    assert_eq!(
        row_set.rows()[0].values(),
        &[Value::Text(expected.to_string())]
    );
}

fn assert_single_integer(result: ExecuteResult, expected: i64) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    assert_eq!(row_set.rows()[0].values(), &[Value::Integer(expected)]);
}

fn assert_single_json(result: ExecuteResult, expected: &str) {
    let row_set = result;
    assert_eq!(row_set.len(), 1);
    let expected_json = serde_json::from_str::<serde_json::Value>(expected)
        .expect("expected JSON value should parse");
    assert_eq!(row_set.rows()[0].values(), &[Value::Json(expected_json)]);
}

fn assert_closed(error: lix_engine::LixError) {
    assert_eq!(error.code, lix_engine::LixError::CODE_CLOSED);
    assert_eq!(error.message, "Lix handle is closed");
    assert_eq!(
        error.hint.as_deref(),
        Some("Open a new Lix handle before calling this method.")
    );
}
