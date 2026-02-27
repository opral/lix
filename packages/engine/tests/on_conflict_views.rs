mod support;

use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text active version id, got {other:?}"),
    }
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_state_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn insert_version(engine: &support::simulation_test::SimulationEngine, version_id: &str) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', 'global', false, 'commit-{version_id}', 'working-{version_id}'\
         )",
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    on_conflict_entity_view_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

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
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "value-b");
    }
);

simulation_test!(
    on_conflict_entity_by_version_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        let version_id = active_version_id(&engine).await;

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
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "value-b");
    }
);

simulation_test!(
    on_conflict_state_view_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state', 'test_state_schema', 'test-file', 'lix', '1', '{\"value\":\"A\"}'\
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
                 'oc-state', 'test_state_schema', 'test-file', 'lix', '1', '{\"value\":\"B\"}'\
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
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    on_conflict_state_by_version_do_update_is_applied,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state-bv', 'test_state_schema', 'test-file', 'version-a', 'lix', '1', '{\"value\":\"A\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'oc-state-bv', 'test_state_schema', 'test-file', 'version-a', 'lix', '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'",
                &[],
            )
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
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    on_conflict_stored_schema_by_version_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let schema_json = "{\"x-lix-key\":\"on_conflict_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}";

        engine
            .execute(
                "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) \
                 VALUES (lix_json(?1), 'global') \
                 ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
                &[Value::Text(schema_json.to_string())],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_stored_schema_by_version (value, lixcol_version_id) \
                 VALUES (lix_json(?1), 'global') \
                 ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
                &[Value::Text(schema_json.to_string())],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_state_materialized_v1_lix_stored_schema \
                 WHERE entity_id = 'on_conflict_schema~1' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][0], Value::Integer(1));
    }
);
