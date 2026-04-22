use crate::support;

use lix_engine::Value;
use serde_json::{json, Value as JsonValue};

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_json_string(value: &Value, expected: &str) {
    match value {
        Value::Json(JsonValue::String(actual)) => assert_eq!(actual, expected),
        other => panic!("expected json string value '{expected}', got {other:?}"),
    }
}

async fn register_test_schema(engine: &support::simulation_test::SimulatedLix) {
    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"test_state_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn ensure_file_descriptor(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
    file_id: &str,
) {
    let existing = engine
        .execute(
            "SELECT entity_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = $1 \
               AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await
        .unwrap();
    if !existing.statements[0].rows.is_empty() {
        return;
    }

    let (name, extension) = file_id
        .rsplit_once('.')
        .map(|(name, extension)| (name, Some(extension)))
        .unwrap_or((file_id, None));
    let snapshot = json!({
        "id": file_id,
        "directory_id": null,
        "name": name,
        "extension": extension,
        "metadata": null,
        "hidden": false
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             $1, 'lix_file_descriptor', NULL, $2, NULL, '1', $3\
             )",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(snapshot),
            ],
        )
        .await
        .unwrap();
}

simulation_test!(
    on_conflict_entity_view_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('oc-entity', 'value-a')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('oc-entity', 'value-b') \
                 ON CONFLICT (key) DO UPDATE SET value = 'value-b'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'oc-entity'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_json_string(&rows.statements[0].rows[0][0], "value-b");
    }
);

simulation_test!(
    on_conflict_entity_by_version_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();
        let version_id = engine.active_version_id().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (key, value, lixcol_version_id) \
                 VALUES ('oc-entity-bv', 'value-a', $1)",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value_by_version (key, value, lixcol_version_id) \
                 VALUES ('oc-entity-bv', 'value-b', $1) \
                 ON CONFLICT (key, lixcol_version_id) DO UPDATE SET value = 'value-b'",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT value \
                 FROM lix_key_value_by_version \
                 WHERE key = 'oc-entity-bv' AND lixcol_version_id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_json_string(&rows.statements[0].rows[0][0], "value-b");
    }
);

simulation_test!(
    on_conflict_state_view_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state', 'test_state_schema', 'test-file', NULL, '1', '{\"value\":\"A\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state', 'test_state_schema', 'test-file', NULL, '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' AND entity_id = 'oc-state'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    on_conflict_state_by_version_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state-bv', 'test_state_schema', 'test-file', 'version-a', NULL, '1', '{\"value\":\"A\"}'\
                 )", &[])
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state-bv', 'test_state_schema', 'test-file', 'version-a', NULL, '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'", &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'oc-state-bv' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    on_conflict_registered_schema_by_version_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let schema_json = "{\"x-lix-key\":\"on_conflict_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}";

        engine
            .execute(
                "INSERT INTO lix_registered_schema_by_version (value, lixcol_version_id) \
                 VALUES (lix_json($1), 'global') \
                 ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
                &[Value::Text(schema_json.to_string())],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema_by_version (value, lixcol_version_id) \
                 VALUES (lix_json($1), 'global') \
                 ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
                &[Value::Text(schema_json.to_string())],
            )
            .await
            .unwrap();
    }
);
