mod support;

use lix_engine::{BootKeyValue, Value};

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn register_test_schema_sql() -> &'static str {
    "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
     'lix_stored_schema',\
     '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
     )"
}

fn register_defaults_schema_sql() -> &'static str {
    "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
     'lix_stored_schema',\
     '{\"value\":{\"x-lix-key\":\"defaults_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"created_at\":{\"type\":\"string\",\"x-lix-default\":\"lix_timestamp()\"}},\"additionalProperties\":false}}'\
     )"
}

fn deterministic_uuid(counter: i64) -> String {
    format!("01920000-0000-7000-8000-0000{counter:08x}")
}

async fn read_sequence_value(engine: &support::simulation_test::SimulationEngine) -> i64 {
    let sequence = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'lix_deterministic_sequence_number' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(sequence.rows.len(), 1);
    let snapshot_content = match &sequence.rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text snapshot_content, got {other:?}"),
    };
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
    parsed["value"]
        .as_i64()
        .expect("sequence value must be integer")
}

simulation_test!(
    deterministic_boot_key_values_apply_during_init,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: vec![BootKeyValue {
                    key: "lix_deterministic_mode".to_string(),
                    value: serde_json::json!({ "enabled": true }),
                    version_id: None,
                }],
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let mode_metadata = engine
            .execute(
                "SELECT created_at, updated_at \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'lix_deterministic_mode' \
                   AND version_id = 'global' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(mode_metadata.rows.len(), 1);
        let created_at = match &mode_metadata.rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text created_at, got {other:?}"),
        };
        let updated_at = match &mode_metadata.rows[0][1] {
            Value::Text(value) => value,
            other => panic!("expected text updated_at, got {other:?}"),
        };
        sim.expect_deterministic(created_at.to_string());
        sim.expect_deterministic(updated_at.to_string());
        assert!(created_at.starts_with("1970-01-01T00:00:00."));
        assert!(created_at.ends_with('Z'));
        assert_eq!(created_at, updated_at);

        let mode_row = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'lix_deterministic_mode' \
                   AND version_id = 'global' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(mode_row.rows.len(), 1);

        let mode_snapshot = match &mode_row.rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text snapshot_content, got {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(mode_snapshot).unwrap();
        assert_eq!(parsed["value"]["enabled"], serde_json::Value::Bool(true));
    }
);

simulation_test!(
    deterministic_functions_are_sequential_and_persisted,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        let first = engine
            .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
            .await
            .unwrap();

        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0][0], Value::Text(deterministic_uuid(0)));
        assert_eq!(
            first.rows[0][1],
            Value::Text("1970-01-01T00:00:00.001Z".to_string())
        );
        assert_eq!(first.rows[0][2], Value::Text(deterministic_uuid(2)));

        let second = engine
            .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
            .await
            .unwrap();

        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0][0], Value::Text(deterministic_uuid(3)));
        assert_eq!(
            second.rows[0][1],
            Value::Text("1970-01-01T00:00:00.004Z".to_string())
        );
        assert_eq!(second.rows[0][2], Value::Text(deterministic_uuid(5)));

        assert_eq!(read_sequence_value(&engine).await, 5);
    }
);

simulation_test!(
    deterministic_mode_applies_to_tracked_vtable_metadata,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(register_test_schema_sql(), &[])
            .await
            .unwrap();
        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{\"key\":\"tracked\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT id, snapshot_id, created_at \
                 FROM lix_internal_change \
                 WHERE entity_id = 'entity-1' AND schema_key = 'test_schema' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.rows.len(), 1);
        assert_eq!(changes.rows[0][0], Value::Text(deterministic_uuid(1)));
        assert_eq!(changes.rows[0][1], Value::Text(deterministic_uuid(0)));
        assert_eq!(
            changes.rows[0][2],
            Value::Text("1970-01-01T00:00:00.002Z".to_string())
        );

        let materialized = engine
            .execute(
                "SELECT change_id, created_at, updated_at \
                 FROM lix_internal_state_materialized_v1_test_schema \
                 WHERE entity_id = 'entity-1' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(materialized.rows.len(), 1);
        assert_eq!(materialized.rows[0][0], Value::Text(deterministic_uuid(1)));
        assert_eq!(
            materialized.rows[0][1],
            Value::Text("1970-01-01T00:00:00.002Z".to_string())
        );
        assert_eq!(
            materialized.rows[0][2],
            Value::Text("1970-01-01T00:00:00.002Z".to_string())
        );

        assert_eq!(read_sequence_value(&engine).await, 2);
    }
);

simulation_test!(
    deterministic_timestamp_can_be_disabled_independently,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql(
                    "lix_deterministic_mode",
                    "{\"enabled\":true,\"timestamp\":false}",
                ),
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text(deterministic_uuid(0)));
        assert_eq!(result.rows[0][2], Value::Text(deterministic_uuid(1)));

        let timestamp = match &result.rows[0][1] {
            Value::Text(value) => value,
            other => panic!("expected text timestamp, got {other:?}"),
        };
        assert_ne!(timestamp, "1970-01-01T00:00:00.001Z");

        assert_eq!(read_sequence_value(&engine).await, 1);
    }
);

simulation_test!(
    deterministic_mode_applies_to_cel_defaults,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(register_defaults_schema_sql(), &[])
            .await
            .unwrap();
        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-defaults', 'defaults_schema', 'file-1', 'version-1', 'lix', '{}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();

        let row = engine
            .execute(
                "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'defaults_schema' \
               AND entity_id = 'entity-defaults' \
               AND version_id = 'version-1' \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(row.rows.len(), 1);
        let snapshot_content = match &row.rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text snapshot_content, got {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(snapshot_content).unwrap();
        let id = parsed["id"].as_str().expect("id default must be string");
        let created_at = parsed["created_at"]
            .as_str()
            .expect("created_at default must be string");

        assert_eq!(id, deterministic_uuid(1));
        assert_eq!(created_at, "1970-01-01T00:00:00.000Z");

        // 2 calls from CEL defaults + 3 calls from tracked write internals.
        assert_eq!(read_sequence_value(&engine).await, 4);
    }
);
