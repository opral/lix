mod support;

use lix_engine::Value;
use serde_json::Value as JsonValue;

fn text_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Text(text) => serde_json::from_str(text).expect("valid json"),
        other => panic!("expected text value, got {other:?}"),
    }
}

simulation_test!(
    same_request_schema_insert_allows_snapshot_validation,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"same_request_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             );\
             INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'same_request_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Ada\"}', '1'\
             )",
                &[],
            )
            .await;

        assert!(result.is_ok(), "{result:?}");

        let stored = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_vtable \
             WHERE schema_key = 'same_request_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        let snapshot = text_to_json(&stored.rows[0][0]);
        assert_eq!(snapshot["name"], JsonValue::String("Ada".to_string()));
    }
);

simulation_test!(
    same_request_stored_schema_foreign_key_uses_pending_target,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let result = engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"same_request_parent\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}}'\
             );\
             INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"same_request_child\",\"x-lix-version\":\"1\",\"x-lix-foreign-keys\":[{\"properties\":[\"/parent_id\"],\"references\":{\"schemaKey\":\"same_request_parent\",\"properties\":[\"/id\"]}}],\"type\":\"object\",\"properties\":{\"parent_id\":{\"type\":\"string\"}},\"required\":[\"parent_id\"],\"additionalProperties\":false}}'\
             )",
                &[],
            )
            .await;

        assert!(result.is_ok(), "{result:?}");

        let count = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_state_materialized_v1_lix_stored_schema \
             WHERE entity_id IN ('same_request_parent~1', 'same_request_child~1')",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(count.rows[0][0], Value::Integer(2));
    }
);

simulation_test!(
    same_request_schema_insert_applies_defaults,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        let result = engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"same_request_default_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\",\"x-lix-default\":\"name + ''-slug''\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'\
             );\
             INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             'entity-1', 'same_request_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1'\
             )",
            &[],
        )
        .await;

        assert!(result.is_ok(), "{result:?}");

        let row = engine
            .execute(
                "SELECT snapshot_content FROM lix_internal_state_vtable \
             WHERE schema_key = 'same_request_default_schema' AND entity_id = 'entity-1'",
                &[],
            )
            .await
            .unwrap();

        let snapshot = text_to_json(&row.rows[0][0]);
        assert_eq!(
            snapshot["slug"],
            JsonValue::String("Sample-slug".to_string())
        );
    }
);
