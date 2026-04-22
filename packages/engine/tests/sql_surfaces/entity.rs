use crate::support;

use lix_engine::Value;
use serde_json::{json, Value as JsonValue};
use support::simulation_test::assert_boolean_like;

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

fn is_true(value: &Value) -> bool {
    match value {
        Value::Boolean(actual) => *actual,
        Value::Integer(actual) => *actual != 0,
        Value::Text(actual) => matches!(actual.trim().to_ascii_lowercase().as_str(), "1" | "true"),
        Value::Null => false,
        other => panic!("expected boolean-like value, got {other:?}"),
    }
}

fn normalize_bool_like_rows(rows: &[Vec<Value>], columns: &[usize]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, value)| {
                    if columns.contains(&index) {
                        Value::Boolean(is_true(value))
                    } else {
                        value.clone()
                    }
                })
                .collect()
        })
        .collect()
}

async fn seed_key_value_row(
    engine: &support::simulation_test::SimulatedLix,
    key: &str,
    value: &str,
    version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', 'lix_key_value', NULL, '{version_id}', NULL, '{snapshot}', '1'\
         )",
        entity_id = key.replace('\'', "''"),
        version_id = version_id.replace('\'', "''"),
        snapshot = format!(
            "{{\"key\":\"{}\",\"value\":\"{}\"}}",
            key.replace('\"', "\\\""),
            value.replace('\"', "\\\"")
        )
        .replace('\'', "''"),
    );
    engine.execute(&sql, &[]).await.unwrap();
}

async fn install_lix_owned_schema(
    engine: &support::simulation_test::SimulatedLix,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
            },
            "required": ["id"],
            "additionalProperties": false,
        }))
        .await
        .unwrap();
}

async fn install_global_override_schema_for_version_override_schema(
    engine: &support::simulation_test::SimulatedLix,
) {
    install_lix_owned_schema(engine, "lix_version_override_schema").await;
}

async fn install_global_override_child_schema(engine: &support::simulation_test::SimulatedLix) {
    install_lix_owned_schema(engine, "lix_version_override_child_schema").await;
}

async fn install_default_values_schema(engine: &support::simulation_test::SimulatedLix) {
    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"lix_default_values_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"\\\"default-id-value\\\"\"}},\"required\":[\"id\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn install_delete_subquery_schemas(engine: &support::simulation_test::SimulatedLix) {
    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"lix_delete_message_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"bundle_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"bundle_id\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"lix_delete_variant_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"message_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"message_id\"],\"additionalProperties\":false}",
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

fn snapshot_field(snapshot: &Value, field: &str) -> String {
    let text = match snapshot {
        Value::Text(value) => value,
        other => panic!("expected snapshot text, got {other:?}"),
    };
    let parsed: JsonValue =
        serde_json::from_str(text).expect("snapshot_content should be valid JSON");
    parsed
        .get(field)
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("snapshot_content missing text field '{field}'"))
        .to_string()
}

simulation_test!(
    lix_entity_view_select_exposes_properties_and_lixcols,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let active_version = engine.active_version_id().await.unwrap();
        seed_key_value_row(&engine, "key-sel", "value-sel", &active_version).await;

        let result = engine
            .execute(
                "SELECT key, value, lixcol_schema_key \
                 FROM lix_key_value \
                 WHERE key = 'key-sel'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(result.statements[0].rows.len(), 1);
        assert_text(&result.statements[0].rows[0][0], "key-sel");
        assert_json_string(&result.statements[0].rows[0][1], "value-sel");
        assert_text(&result.statements[0].rows[0][2], "lix_key_value");
    }
);

simulation_test!(
    lix_entity_view_insert_update_delete_delegate_to_lix_state,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'key-write', 'value-insert', NULL, NULL, '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_key_value \
                 SET value = 'value-update' \
                 WHERE key = 'key-write'",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'key-write'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_json_string(&updated.statements[0].rows[0][0], "value-update");

        engine
            .execute("DELETE FROM lix_key_value WHERE key = 'key-write'", &[])
            .await
            .unwrap();

        let deleted = engine
            .execute("SELECT key FROM lix_key_value WHERE key = 'key-write'", &[])
            .await
            .unwrap();
        sim.assert_deterministic(deleted.statements[0].rows.clone());
        assert!(deleted.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_entity_view_update_supports_non_identity_state_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'key-state-update', 'value-before', NULL, NULL, '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_key_value \
                 SET value = 'value-after', \
                     lixcol_metadata = '{\"source\":\"update\"}' \
                 WHERE key = 'key-state-update'",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                "SELECT snapshot_content, plugin_key, metadata \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'key-state-update' \
                   AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_eq!(
            snapshot_field(&updated.statements[0].rows[0][0], "value"),
            "value-after".to_string()
        );
        assert_eq!(updated.statements[0].rows[0][1], Value::Null);
        assert_text(&updated.statements[0].rows[0][2], "{\"source\":\"update\"}");
    }
);

simulation_test!(
    lix_entity_view_insert_on_conflict_do_update_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('key-upsert', 'value-a')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('key-upsert', 'value-b') \
                 ON CONFLICT (key) DO UPDATE SET value = 'value-b'",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'key-upsert'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_json_string(&updated.statements[0].rows[0][0], "value-b");
    }
);

simulation_test!(
    lix_entity_view_insert_on_conflict_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('key-upsert', 'value-a')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('key-upsert', 'value-b') \
                 ON CONFLICT (key) DO NOTHING",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'key-upsert'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_json_string(&rows.statements[0].rows[0][0], "value-a");
    }
);

simulation_test!(
    lix_entity_view_insert_default_values_populates_schema_defaults,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();
        install_default_values_schema(&engine).await;

        engine
            .execute("INSERT INTO lix_default_values_schema DEFAULT VALUES", &[])
            .await
            .unwrap();
        let active_version_id = engine.active_version_id().await.unwrap();

        let selected = engine
            .execute(
                "SELECT id FROM lix_default_values_schema WHERE id = 'default-id-value'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(selected.statements[0].rows.clone());
        assert_eq!(selected.statements[0].rows.len(), 1);
        assert_text(&selected.statements[0].rows[0][0], "default-id-value");

        let stored = engine
            .execute(
                &format!(
                    "SELECT entity_id, snapshot_content \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_default_values_schema' \
                       AND version_id = '{active_version_id}' \
                       AND snapshot_content IS NOT NULL"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(stored.statements[0].rows.len(), 1);
        assert_text(&stored.statements[0].rows[0][0], "default-id-value");
        assert_eq!(
            snapshot_field(&stored.statements[0].rows[0][1], "id"),
            "default-id-value".to_string()
        );
    }
);

simulation_test!(
    lix_entity_view_delete_rewrites_property_subquery_predicates,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();
        install_delete_subquery_schemas(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_delete_message_schema (id, bundle_id) VALUES \
                 ('msg-1', 'bundle.cleanup'), ('msg-2', 'bundle.keep')",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_delete_variant_schema (id, message_id) VALUES \
                 ('variant-1', 'msg-1'), ('variant-2', 'msg-2')",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_delete_variant_schema \
                 WHERE message_id IN ( \
                   SELECT id \
                   FROM lix_delete_message_schema \
                   WHERE bundle_id = 'bundle.cleanup' \
                 )",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT id, message_id \
                 FROM lix_delete_variant_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[3]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "variant-2");
        assert_text(&rows.statements[0].rows[0][1], "msg-2");
    }
);

simulation_test!(
    lix_entity_by_version_view_reads_visible_global_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine.create_named_version("version-child").await.unwrap();

        seed_key_value_row(&engine, "inherit-key", "from-global", "global").await;

        let rows = engine
            .execute(
                "SELECT key, value, lixcol_version_id, lixcol_global \
                 FROM lix_key_value_by_version \
                 WHERE key = 'inherit-key' \
                   AND lixcol_version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[3]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "inherit-key");
        assert_json_string(&rows.statements[0].rows[0][1], "from-global");
        assert_text(&rows.statements[0].rows[0][2], "version-child");
        assert_boolean_like(&rows.statements[0].rows[0][3], true);
    }
);

simulation_test!(
    lix_entity_view_update_property_assignment_is_schema_validated,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .register_schema(
                &serde_json::from_str::<serde_json::Value>(
                    "{\"x-lix-key\":\"lix_patch_validation\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false}",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_patch_validation (\
                 id, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'entity-1', 'valid', NULL, NULL, '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "UPDATE lix_patch_validation \
                 SET value = 42 \
                 WHERE id = 'entity-1'",
                &[],
            )
            .await
            .expect_err("expected schema validation failure");
        assert!(
            err.description.contains("is not of type \"string\""),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_entity_view_base_insert_read_defaults_to_active_version_scope,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();
        install_global_override_schema_for_version_override_schema(&engine).await;

        engine.create_named_version("active-test").await.unwrap();
        engine
            .switch_version("active-test".to_string())
            .await
            .unwrap();
        let active_version_id = engine.active_version_id().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version_override_schema (\
                 id, name, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'ovr-1', 'Original', NULL, NULL, '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let stored = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_override_schema' \
                   AND entity_id = 'ovr-1' \
                   AND version_id = $1 \
                   AND snapshot_content IS NOT NULL",
                &[Value::Text(active_version_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(stored.statements[0].rows.len(), 1);
        assert_text(&stored.statements[0].rows[0][0], &active_version_id);
        assert_eq!(
            snapshot_field(&stored.statements[0].rows[0][1], "name"),
            "Original"
        );

        let selected = engine
            .execute(
                "SELECT id, name \
                 FROM lix_version_override_schema \
                 WHERE id = 'ovr-1'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(selected.statements[0].rows.clone());
        assert_eq!(selected.statements[0].rows.len(), 1);
        assert_text(&selected.statements[0].rows[0][0], "ovr-1");
        assert_text(&selected.statements[0].rows[0][1], "Original");
    }
);

simulation_test!(
    lix_account_insert_defaults_to_active_version_scope,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine.create_named_version("account-local").await.unwrap();
        engine
            .switch_version("account-local".to_string())
            .await
            .unwrap();
        let active_version_id = engine.active_version_id().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_account (id, name) VALUES ('acct-local', 'Local Account')",
                &[],
            )
            .await
            .unwrap();

        let stored = engine
            .execute(
                "SELECT version_id, global, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_account' \
                   AND entity_id = 'acct-local' \
                   AND version_id = $1 \
                   AND snapshot_content IS NOT NULL",
                &[Value::Text(active_version_id.clone())],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&stored.statements[0].rows, &[1]));
        assert_eq!(stored.statements[0].rows.len(), 1);
        assert_text(&stored.statements[0].rows[0][0], &active_version_id);
        assert_boolean_like(&stored.statements[0].rows[0][1], false);
        assert_eq!(
            snapshot_field(&stored.statements[0].rows[0][2], "name"),
            "Local Account"
        );

        let selected = engine
            .execute(
                "SELECT id, name, lixcol_global \
                 FROM lix_account \
                 WHERE id = 'acct-local'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&selected.statements[0].rows, &[2]));
        assert_eq!(selected.statements[0].rows.len(), 1);
        assert_text(&selected.statements[0].rows[0][0], "acct-local");
        assert_text(&selected.statements[0].rows[0][1], "Local Account");
        assert_boolean_like(&selected.statements[0].rows[0][2], false);
    }
);

simulation_test!(
    lix_account_insert_can_target_global_scope_explicitly,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine.create_named_version("account-global").await.unwrap();
        engine
            .switch_version("account-global".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_account (id, name, lixcol_global) \
                 VALUES ('acct-global', 'Global Account', true)",
                &[],
            )
            .await
            .unwrap();

        let stored = engine
            .execute(
                "SELECT version_id, global, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_account' \
                   AND entity_id = 'acct-global' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&stored.statements[0].rows, &[1]));
        assert_eq!(stored.statements[0].rows.len(), 1);
        assert_text(&stored.statements[0].rows[0][0], "global");
        assert_boolean_like(&stored.statements[0].rows[0][1], true);
        assert_eq!(
            snapshot_field(&stored.statements[0].rows[0][2], "name"),
            "Global Account"
        );

        let selected = engine
            .execute(
                "SELECT id, name, lixcol_global \
                 FROM lix_account \
                 WHERE id = 'acct-global'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&selected.statements[0].rows, &[2]));
        assert_eq!(selected.statements[0].rows.len(), 1);
        assert_text(&selected.statements[0].rows[0][0], "acct-global");
        assert_text(&selected.statements[0].rows[0][1], "Global Account");
        assert_boolean_like(&selected.statements[0].rows[0][2], true);
    }
);

simulation_test!(
    lix_entity_view_base_select_includes_global_fallback_and_active_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();
        install_global_override_child_schema(&engine).await;
        engine.create_named_version("active-inherit").await.unwrap();
        engine
            .switch_version("active-inherit".to_string())
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'ovr-global-1', 'lix_version_override_child_schema', NULL, 'global', NULL, '{\"id\":\"ovr-global-1\",\"name\":\"Global\"}', '1'\
                 )", &[])
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'ovr-active-1', 'lix_version_override_child_schema', NULL, lix_active_version_id(), NULL, '{\"id\":\"ovr-active-1\",\"name\":\"Active\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT id, name, lixcol_global \
                 FROM lix_version_override_child_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[2]));
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "ovr-active-1");
        assert_text(&rows.statements[0].rows[0][1], "Active");
        assert_boolean_like(&rows.statements[0].rows[0][2], false);
        assert_text(&rows.statements[0].rows[1][0], "ovr-global-1");
        assert_text(&rows.statements[0].rows[1][1], "Global");
        assert_boolean_like(&rows.statements[0].rows[1][2], true);
    }
);

simulation_test!(
    lix_entity_view_insert_rejects_unknown_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_key_value (key, value, bogus) VALUES ('k-unknown', 'v-unknown', 'x')", &[])
            .await
            .expect_err("insert with unknown column should fail");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_entity_view_update_rejects_unknown_assignment_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'k-update-unknown', 'v', NULL, NULL, '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "UPDATE lix_key_value SET bogus = 'x' WHERE key = 'k-update-unknown'",
                &[],
            )
            .await
            .expect_err("update with unknown column should fail");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_entity_view_delete_rejects_unknown_where_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute("DELETE FROM lix_key_value WHERE bogus = 'x'", &[])
            .await
            .expect_err("delete with unknown predicate column should fail");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_entity_view_read_unknown_column_lists_builtin_entity_view_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute("SELECT bogus FROM lix_key_value LIMIT 1", &[])
            .await
            .expect_err("read with unknown column should fail");

        assert_eq!(err.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(err.description.contains("on `lix_key_value`"));
        assert!(err.description.contains("Available columns: key"));
        assert!(err.description.contains("value"));
        assert!(err.description.contains("lixcol_entity_id"));
        assert!(!err.description.contains("Available columns: (unknown)."));
    }
);

simulation_test!(
    lix_entity_view_read_unknown_column_lists_custom_entity_view_columns,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_registered_schema_by_version (value, lixcol_version_id) VALUES (\
                 lix_json('{\"x-lix-key\":\"lix_custom_error_columns\",\"x-lix-version\":\"1\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"]}'),\
                 'global'\
                 )", &[])
            .await
            .expect("schema insert should succeed");

        let err = engine
            .execute("SELECT bogus FROM lix_custom_error_columns LIMIT 1", &[])
            .await
            .expect_err("read with unknown column should fail");

        assert_eq!(err.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");
        assert!(err.description.contains("on `lix_custom_error_columns`"));
        assert!(err.description.contains("Available columns:"));
    }
);

simulation_test!(
    write_routing_rejects_unsupported_non_lix_targets,
    |sim| async move {
        let insert_engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        insert_engine.initialize().await.unwrap();

        let insert_err = insert_engine
            .execute(
                "INSERT INTO some_non_lix_table (id, value) VALUES ('x', 'y')",
                &[],
            )
            .await
            .expect_err("insert into unsupported target should fail");
        assert!(
            insert_err.description.contains("some_non_lix_table")
                && (insert_err.description.contains("does not exist")
                    || insert_err.description.contains("no such table")),
            "unexpected insert error: {}",
            insert_err.description
        );

        let update_engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        update_engine.initialize().await.unwrap();

        let update_err = update_engine
            .execute(
                "UPDATE some_non_lix_table SET value = 'z' WHERE id = 'x'",
                &[],
            )
            .await
            .expect_err("update on unsupported target should fail");
        assert!(
            update_err.description.contains("some_non_lix_table")
                && (update_err.description.contains("does not exist")
                    || update_err.description.contains("no such table")),
            "unexpected update error: {}",
            update_err.description
        );

        let delete_engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        delete_engine.initialize().await.unwrap();

        let delete_err = delete_engine
            .execute("DELETE FROM some_non_lix_table WHERE id = 'x'", &[])
            .await
            .expect_err("delete on unsupported target should fail");
        assert!(
            delete_err.description.contains("some_non_lix_table")
                && (delete_err.description.contains("does not exist")
                    || delete_err.description.contains("no such table")),
            "unexpected delete error: {}",
            delete_err.description
        );
    }
);
