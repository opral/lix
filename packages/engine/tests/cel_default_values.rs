mod support;

use chrono::DateTime;
use lix_engine::Value;
use serde_json::Value as JsonValue;
use support::simulation_test::SimulationArgs;
use uuid::Uuid;

async fn insert_schema(engine: &support::simulation_test::SimulationEngine, schema: &str) {
    engine
        .execute(
            &format!(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{}')",
                schema.replace('\'', "''")
            ),
            &[],
        )
        .await
        .unwrap();
}

fn text_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Text(text) => serde_json::from_str(text).expect("valid json"),
        other => panic!("expected text value, got {other:?}"),
    }
}

async fn enable_deterministic_mode(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'lix_deterministic_mode', 'lix_key_value', 'lix', 'global', 'lix', '{\"key\":\"lix_deterministic_mode\",\"value\":{\"enabled\":true}}', '1'\
             )",
            &[],
        )
        .await
        .unwrap();
}

fn deterministic_uuid(counter: i64) -> String {
    let counter_bits = (counter as u64) & 0x0000_FFFF_FFFF_FFFF;
    format!("01920000-0000-7000-8000-{counter_bits:012x}")
}

simulation_test!(insert_applies_cel_default, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"cel_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"slug":{"type":"string","x-lix-default":"name + '-slug'"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'cel_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'cel_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    assert_eq!(snapshot["name"], JsonValue::String("Sample".to_string()));
    assert_eq!(
        snapshot["slug"],
        JsonValue::String("Sample-slug".to_string())
    );
});

simulation_test!(
    insert_applies_cel_default_with_parameterized_snapshot_content,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        insert_schema(
            &engine,
            r#"{"value":{"x-lix-key":"cel_default_param_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"slug":{"type":"string","x-lix-default":"name + '-slug'"}},"required":["name"],"additionalProperties":false}}"#,
        )
        .await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Value::Text("entity-1".to_string()),
                    Value::Text("cel_default_param_schema".to_string()),
                    Value::Text("file-1".to_string()),
                    Value::Text("version-1".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("{\"name\":\"Sample\"}".to_string()),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .unwrap();

        let row = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'cel_default_param_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        let snapshot = text_to_json(&row.rows[0][0]);
        assert_eq!(snapshot["name"], JsonValue::String("Sample".to_string()));
        assert_eq!(
            snapshot["slug"],
            JsonValue::String("Sample-slug".to_string())
        );
    }
);

simulation_test!(
    insert_select_applies_cel_default_with_parameterized_source,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        insert_schema(
            &engine,
            r#"{"value":{"x-lix-key":"cel_default_select_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"slug":{"type":"string","x-lix-default":"name + '-slug'"}},"required":["name"],"additionalProperties":false}}"#,
        )
        .await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) SELECT $1, $2, $3, $4, $5, $6, $7",
                &[
                    Value::Text("entity-1".to_string()),
                    Value::Text("cel_default_select_schema".to_string()),
                    Value::Text("file-1".to_string()),
                    Value::Text("version-1".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("{\"name\":\"Sample\"}".to_string()),
                    Value::Text("1".to_string()),
                ],
            )
            .await
            .unwrap();

        let row = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'cel_default_select_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        let snapshot = text_to_json(&row.rows[0][0]);
        assert_eq!(snapshot["name"], JsonValue::String("Sample".to_string()));
        assert_eq!(
            snapshot["slug"],
            JsonValue::String("Sample-slug".to_string())
        );
    }
);

simulation_test!(insert_uses_json_default_fallback, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"json_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"status":{"type":"string","default":"pending"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'json_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'json_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    assert_eq!(snapshot["status"], JsonValue::String("pending".to_string()));
});

simulation_test!(insert_x_lix_default_overrides_default, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"override_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"status":{"type":"string","default":"pending","x-lix-default":"'computed'"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'override_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'override_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    assert_eq!(
        snapshot["status"],
        JsonValue::String("computed".to_string())
    );
});

simulation_test!(insert_does_not_override_explicit_null, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"null_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"status":{"anyOf":[{"type":"string"},{"type":"null"}],"x-lix-default":"'computed'"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'null_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\",\"status\":null}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'null_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    assert_eq!(snapshot["status"], JsonValue::Null);
});

simulation_test!(update_does_not_backfill_defaults, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"update_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"slug":{"type":"string","x-lix-default":"name + '-slug'"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'update_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "UPDATE lix_internal_state_vtable SET snapshot_content = '{\"name\":\"Renamed\"}' WHERE schema_key = 'update_default_schema' AND entity_id = 'entity-1' AND file_id = 'file-1' AND version_id = 'version-1'",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'update_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    assert_eq!(snapshot["name"], JsonValue::String("Renamed".to_string()));
    assert!(snapshot.get("slug").is_none());
});

async fn run_insert_applies_uuid_function_default(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();
    enable_deterministic_mode(&engine).await;

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"uuid_fn_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"token":{"type":"string","x-lix-default":"lix_uuid_v7()"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'uuid_fn_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'uuid_fn_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    let token = snapshot["token"].as_str().expect("token to be string");
    sim.assert_deterministic(token.to_string());
    assert_eq!(token, deterministic_uuid(0));
    Uuid::parse_str(token).expect("token to be valid UUID");
}

simulation_test!(
    insert_applies_uuid_function_default,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        run_insert_applies_uuid_function_default(sim).await;
    }
);

async fn run_insert_applies_timestamp_function_default(sim: SimulationArgs) {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();
    enable_deterministic_mode(&engine).await;

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"timestamp_fn_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"created_at":{"type":"string","x-lix-default":"lix_timestamp()"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'timestamp_fn_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await
        .unwrap();

    let row = engine
        .execute(
            "SELECT snapshot_content FROM lix_internal_state_vtable WHERE schema_key = 'timestamp_fn_default_schema' AND entity_id = 'entity-1'",
            &[],
        )
        .await
        .unwrap();

    let snapshot = text_to_json(&row.rows[0][0]);
    let created_at = snapshot["created_at"]
        .as_str()
        .expect("created_at to be string");
    sim.assert_deterministic(created_at.to_string());
    assert_eq!(created_at, "1970-01-01T00:00:00.000Z");
    DateTime::parse_from_rfc3339(created_at).expect("created_at to be strict RFC3339");
    assert!(created_at.ends_with('Z'));
    let fraction = created_at
        .split('.')
        .nth(1)
        .expect("created_at has millisecond fraction");
    assert_eq!(fraction.len(), 4);
}

simulation_test!(
    insert_applies_timestamp_function_default,
    simulations = [sqlite, postgres, materialization],
    |sim| async move {
        run_insert_applies_timestamp_function_default(sim).await;
    }
);

simulation_test!(insert_fails_on_unknown_cel_variable, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    insert_schema(
        &engine,
        r#"{"value":{"x-lix-key":"unknown_var_default_schema","x-lix-version":"1","type":"object","properties":{"name":{"type":"string"},"slug":{"type":"string","x-lix-default":"missing_var + '-slug'"}},"required":["name"],"additionalProperties":false}}"#,
    )
    .await;

    let result = engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) VALUES ('entity-1', 'unknown_var_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
            &[],
        )
        .await;

    let err = result.expect_err("expected unknown CEL variable error");
    let message = err.to_string();
    assert!(message.contains("failed to evaluate x-lix-default"));
    assert!(message.contains("Undeclared reference"));
});
