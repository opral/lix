mod support;

use chrono::DateTime;
use lix_engine::Value;
use serde_json::json;
use uuid::Uuid;

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

fn deterministic_uuid(counter: i64) -> String {
    let counter_bits = (counter as u64) & 0x0000_FFFF_FFFF_FFFF;
    format!("01920000-0000-7000-8000-{counter_bits:012x}")
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .register_schema(&json!({
            "x-lix-key": "test_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/key"],
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\"",
                "lixcol_global": "true"
            },
            "type": "object",
            "properties": {
                "key": { "type": "string" }
            },
            "required": ["key"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn register_defaults_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .register_schema(&json!({
            "x-lix-key": "defaults_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\"",
                "lixcol_global": "true"
            },
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "created_at": { "type": "string", "x-lix-default": "lix_timestamp()" }
            },
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn read_sequence_value(engine: &support::simulation_test::SimulationEngine) -> i64 {
    let sequence = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'lix_deterministic_sequence_number' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(sequence.statements[0].rows.len(), 1);
    let snapshot_content = match &sequence.statements[0].rows[0][0] {
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
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        let mode_metadata = engine
            .execute(
                "SELECT created_at, updated_at \
                 FROM lix_internal_live_v1_lix_key_value \
                 WHERE entity_id = 'lix_deterministic_mode' \
                   AND version_id = 'global' \
                   AND untracked = true \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(mode_metadata.statements[0].rows.len(), 1);
        let created_at = match &mode_metadata.statements[0].rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text created_at, got {other:?}"),
        };
        let updated_at = match &mode_metadata.statements[0].rows[0][1] {
            Value::Text(value) => value,
            other => panic!("expected text updated_at, got {other:?}"),
        };
        sim.assert_deterministic(created_at.to_string());
        sim.assert_deterministic(updated_at.to_string());
        assert!(created_at.starts_with("1970-01-01T00:00:00."));
        assert!(created_at.ends_with('Z'));
        assert_eq!(created_at, updated_at);

        let mode_row = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'lix_deterministic_mode' \
                   AND version_id = 'global' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(mode_row.statements[0].rows.len(), 1);

        let mode_snapshot = match &mode_row.statements[0].rows[0][0] {
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
        engine.initialize().await.unwrap();

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

        assert_eq!(first.statements[0].rows.len(), 1);
        assert_eq!(
            first.statements[0].rows[0][0],
            Value::Text(deterministic_uuid(0))
        );
        assert_eq!(
            first.statements[0].rows[0][1],
            Value::Text("1970-01-01T00:00:00.001Z".to_string())
        );
        assert_eq!(
            first.statements[0].rows[0][2],
            Value::Text(deterministic_uuid(2))
        );

        let second = engine
            .execute("SELECT lix_uuid_v7(), lix_timestamp(), lix_uuid_v7()", &[])
            .await
            .unwrap();

        assert_eq!(second.statements[0].rows.len(), 1);
        assert_eq!(
            second.statements[0].rows[0][0],
            Value::Text(deterministic_uuid(3))
        );
        assert_eq!(
            second.statements[0].rows[0][1],
            Value::Text("1970-01-01T00:00:00.004Z".to_string())
        );
        assert_eq!(
            second.statements[0].rows[0][2],
            Value::Text(deterministic_uuid(5))
        );

        assert_eq!(read_sequence_value(&engine).await, 5);
    }
);

simulation_test!(
    deterministic_mode_applies_to_tracked_metadata,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute("INSERT INTO test_schema (key) VALUES ('tracked')", &[])
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT id, snapshot_id, created_at \
                 FROM lix_internal_change \
                 WHERE entity_id = 'tracked' AND schema_key = 'test_schema' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(changes.statements[0].rows.len(), 1);
        assert_eq!(
            changes.statements[0].rows[0][0],
            Value::Text(deterministic_uuid(1))
        );
        assert_eq!(
            changes.statements[0].rows[0][1],
            Value::Text(deterministic_uuid(9))
        );
        assert_eq!(
            changes.statements[0].rows[0][2],
            Value::Text("1970-01-01T00:00:00.000Z".to_string())
        );

        let materialized = engine
            .execute(
                "SELECT lixcol_change_id, lixcol_created_at, lixcol_updated_at \
                 FROM test_schema \
                 WHERE key = 'tracked' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(materialized.statements[0].rows.len(), 1);
        assert_eq!(
            materialized.statements[0].rows[0][0],
            Value::Text(deterministic_uuid(1))
        );
        assert_eq!(
            materialized.statements[0].rows[0][1],
            Value::Text("1970-01-01T00:00:00.000Z".to_string())
        );
        assert_eq!(
            materialized.statements[0].rows[0][2],
            Value::Text("1970-01-01T00:00:00.000Z".to_string())
        );

        // Tracked writes now persist the full canonical runtime sequence,
        // including the additional snapshot ids allocated while materializing
        // the canonical change log.
        assert_eq!(read_sequence_value(&engine).await, 11);
    }
);

simulation_test!(
    deterministic_timestamp_can_be_disabled_independently,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

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

        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(
            result.statements[0].rows[0][0],
            Value::Text(deterministic_uuid(0))
        );
        assert_eq!(
            result.statements[0].rows[0][2],
            Value::Text(deterministic_uuid(1))
        );

        let timestamp = match &result.statements[0].rows[0][1] {
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
        engine.initialize().await.unwrap();

        register_defaults_schema(&engine).await;
        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        engine
            .execute("INSERT INTO defaults_schema DEFAULT VALUES", &[])
            .await
            .unwrap();

        let row = engine
            .execute(
                "SELECT snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'defaults_schema' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(row.statements[0].rows.len(), 1);
        let snapshot_content = match &row.statements[0].rows[0][0] {
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

        // The entity-surface path now shares the full tracked write contract,
        // including canonical snapshot-id allocation.
        assert_eq!(read_sequence_value(&engine).await, 13);
    }
);

simulation_test!(
    deterministic_uuid_stays_valid_after_u32_sequence_boundary,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_sequence_number", "4294967295"),
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                &insert_key_value_sql("lix_deterministic_mode", "{\"enabled\":true}"),
                &[],
            )
            .await
            .unwrap();

        let result = engine.execute("SELECT lix_uuid_v7()", &[]).await.unwrap();

        assert_eq!(result.statements[0].rows.len(), 1);
        let uuid = match &result.statements[0].rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text uuid, got {other:?}"),
        };
        assert_eq!(uuid, &deterministic_uuid(4_294_967_296));
        assert_eq!(uuid.len(), 36);
        Uuid::parse_str(uuid).expect("deterministic uuid to remain parseable after u32 overflow");

        assert_eq!(read_sequence_value(&engine).await, 4_294_967_296);
    }
);

simulation_test!(
    timestamp_shuffle_simulation_produces_non_monotonic_timestamps,
    simulations = [timestamp_shuffle],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        let before = read_sequence_value(&engine).await;
        let mut millis = Vec::new();
        for _ in 0..12 {
            let result = engine.execute("SELECT lix_timestamp()", &[]).await.unwrap();
            assert_eq!(result.statements[0].rows.len(), 1);
            let timestamp = match &result.statements[0].rows[0][0] {
                Value::Text(value) => value,
                other => panic!("expected text timestamp, got {other:?}"),
            };
            let parsed =
                DateTime::parse_from_rfc3339(timestamp).expect("timestamp should be valid RFC3339");
            millis.push(parsed.timestamp_millis());
        }

        let mut found_non_monotonic_step = false;
        for pair in millis.windows(2) {
            if pair[1] < pair[0] {
                found_non_monotonic_step = true;
                break;
            }
        }
        assert!(
            found_non_monotonic_step,
            "timestamp shuffle simulation should produce at least one out-of-order timestamp"
        );

        let after = read_sequence_value(&engine).await;
        assert_eq!(after, before + 12);
    }
);
