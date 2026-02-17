mod support;

use lix_engine::Value;
use serde_json::{json, Value as JsonValue};

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

async fn seed_key_value_row(
    engine: &support::simulation_test::SimulationEngine,
    key: &str,
    value: &str,
    version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', 'lix_key_value', 'lix', '{version_id}', 'lix', '{snapshot}', '1'\
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

async fn install_version_override_schema_with_version(
    engine: &support::simulation_test::SimulationEngine,
    schema_key: &str,
    version_id: &str,
) {
    let snapshot = json!({
        "value": {
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\"",
                "lixcol_version_id": format!("\"{version_id}\""),
            },
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
            },
            "required": ["id"],
            "additionalProperties": false,
        }
    });
    let sql = format!(
        "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
         'lix_stored_schema', '{snapshot}'\
         )",
        snapshot = snapshot.to_string().replace('\'', "''"),
    );
    engine
        .execute(&sql, &[])
        .await
        .unwrap();
}

async fn install_version_override_schema(engine: &support::simulation_test::SimulationEngine) {
    install_version_override_schema_with_version(engine, "lix_version_override_schema", "global")
        .await;
}

async fn install_child_version_override_schema(
    engine: &support::simulation_test::SimulationEngine,
) {
    install_version_override_schema_with_version(
        engine,
        "lix_version_override_child_schema",
        "version-child",
    )
    .await;
}

async fn install_select_override_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema', \
             '{\"value\":{\"x-lix-key\":\"lix_select_override_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"inlang\\\"\",\"lixcol_plugin_key\":\"\\\"inlang_sdk\\\"\",\"lixcol_version_id\":\"\\\"global\\\"\",\"lixcol_untracked\":\"1\",\"lixcol_metadata\":\"null\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn install_inherited_override_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema', \
             '{\"value\":{\"x-lix-key\":\"lix_inherited_override_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_inherited_from_version_id\":\"\\\"global\\\"\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn install_default_values_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema', \
             '{\"value\":{\"x-lix-key\":\"lix_default_values_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"lix\\\"\",\"lixcol_plugin_key\":\"\\\"lix\\\"\",\"lixcol_version_id\":\"\\\"global\\\"\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"\\\"default-id-value\\\"\"}},\"required\":[\"id\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn install_delete_subquery_schemas(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema', \
             '{\"value\":{\"x-lix-key\":\"lix_delete_message_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"lix\\\"\",\"lixcol_plugin_key\":\"\\\"lix\\\"\",\"lixcol_version_id\":\"\\\"global\\\"\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"bundle_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"bundle_id\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();

    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema', \
             '{\"value\":{\"x-lix-key\":\"lix_delete_variant_schema\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"x-lix-override-lixcols\":{\"lixcol_file_id\":\"\\\"lix\\\"\",\"lixcol_plugin_key\":\"\\\"lix\\\"\",\"lixcol_version_id\":\"\\\"global\\\"\"},\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"message_id\":{\"type\":\"string\"}},\"required\":[\"id\",\"message_id\"],\"additionalProperties\":false}}'\
             )",
            &[],
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
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        let active_version = active_version_id(&engine).await;
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

        sim.assert_deterministic(result.rows.clone());
        assert_eq!(result.rows.len(), 1);
        assert_text(&result.rows[0][0], "key-sel");
        assert_text(&result.rows[0][1], "value-sel");
        assert_text(&result.rows[0][2], "lix_key_value");
    }
);

simulation_test!(
    lix_entity_view_insert_update_delete_delegate_to_lix_state,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'key-write', 'value-insert', 'lix', 'lix', '1'\
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
        sim.assert_deterministic(updated.rows.clone());
        assert_eq!(updated.rows.len(), 1);
        assert_text(&updated.rows[0][0], "value-update");

        engine
            .execute("DELETE FROM lix_key_value WHERE key = 'key-write'", &[])
            .await
            .unwrap();

        let deleted = engine
            .execute("SELECT key FROM lix_key_value WHERE key = 'key-write'", &[])
            .await
            .unwrap();
        sim.assert_deterministic(deleted.rows.clone());
        assert!(deleted.rows.is_empty());
    }
);

simulation_test!(
    lix_entity_view_insert_default_values_populates_schema_defaults,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_default_values_schema(&engine).await;

        engine
            .execute("INSERT INTO lix_default_values_schema DEFAULT VALUES", &[])
            .await
            .unwrap();

        let selected = engine
            .execute(
                "SELECT id FROM lix_default_values_schema WHERE id = 'default-id-value'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(selected.rows.clone());
        assert_eq!(selected.rows.len(), 1);
        assert_text(&selected.rows[0][0], "default-id-value");

        let stored = engine
            .execute(
                "SELECT entity_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_default_values_schema'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(stored.rows.len(), 1);
        assert_text(&stored.rows[0][0], "default-id-value");
        assert_eq!(
            snapshot_field(&stored.rows[0][1], "id"),
            "default-id-value".to_string()
        );
    }
);

simulation_test!(
    lix_entity_view_delete_rewrites_property_subquery_predicates,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
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
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "variant-2");
        assert_text(&rows.rows[0][1], "msg-2");
    }
);

simulation_test!(
    lix_entity_by_version_view_inherits_from_parent_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'version-child', 'version-child', 'global', 0, 'commit-child', 'working-child'\
                 )",
                &[],
            )
            .await
            .unwrap();

        seed_key_value_row(&engine, "inherit-key", "from-global", "global").await;

        let rows = engine
            .execute(
                "SELECT key, value, lixcol_version_id, lixcol_inherited_from_version_id \
                 FROM lix_key_value_by_version \
                 WHERE key = 'inherit-key' \
                   AND lixcol_version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "inherit-key");
        assert_text(&rows.rows[0][1], "from-global");
        assert_text(&rows.rows[0][2], "version-child");
        assert_text(&rows.rows[0][3], "global");
    }
);

simulation_test!(
    lix_entity_view_update_property_assignment_is_schema_validated,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
                 'lix_stored_schema', \
                 '{\"value\":{\"x-lix-key\":\"lix_patch_validation\",\"x-lix-version\":\"1\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}},\"required\":[\"id\",\"value\"],\"additionalProperties\":false}}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_patch_validation (\
                 id, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'entity-1', 'valid', 'lix', 'lix', '1'\
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
            err.message
                .contains("snapshot_content does not match schema 'lix_patch_validation'"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    lix_entity_view_base_insert_read_honors_lixcol_version_id_override,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_version_override_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'active-test', 'active-test', NULL, 0, 'commit-active', 'working-active'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'active-test'",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_version_override_schema (\
                 id, name, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'ovr-1', 'Original', 'lix', 'lix', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let stored = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_override_schema' \
                   AND entity_id = 'ovr-1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(stored.rows.len(), 1);
        assert_text(&stored.rows[0][0], "global");
        assert_eq!(snapshot_field(&stored.rows[0][1], "name"), "Original");

        let selected = engine
            .execute(
                "SELECT id, name \
                 FROM lix_version_override_schema \
                 WHERE id = 'ovr-1'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(selected.rows.clone());
        assert_eq!(selected.rows.len(), 1);
        assert_text(&selected.rows[0][0], "ovr-1");
        assert_text(&selected.rows[0][1], "Original");
    }
);

simulation_test!(
    lix_entity_view_base_update_honors_lixcol_version_id_override,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_version_override_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'ovr-2', 'lix_version_override_schema', 'lix', 'global', 'lix', '{\"id\":\"ovr-2\",\"name\":\"Global\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'ovr-2', 'lix_version_override_schema', 'lix', 'main', 'lix', '{\"id\":\"ovr-2\",\"name\":\"Main\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_version_override_schema \
                 SET name = 'Updated' \
                 WHERE id = 'ovr-2'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_override_schema' \
                   AND entity_id = 'ovr-2' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "global");
        let global_name = snapshot_field(&rows.rows[0][1], "name");
        assert_eq!(global_name, "Updated");
        assert_text(&rows.rows[1][0], "main");
        let main_name = snapshot_field(&rows.rows[1][1], "name");
        assert_eq!(main_name, "Main");
        sim.assert_deterministic(vec![vec![Value::Text(global_name), Value::Text(main_name)]]);
    }
);

simulation_test!(
    lix_entity_view_base_select_with_lixcol_version_id_override_inherits_parent_state,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_child_version_override_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'version-child', 'version-child', 'global', 0, 'commit-child', 'working-child'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'ovr-inherit-1', 'lix_version_override_child_schema', 'lix', 'global', 'lix', '{\"id\":\"ovr-inherit-1\",\"name\":\"Global\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT id, name, lixcol_inherited_from_version_id \
                 FROM lix_version_override_child_schema \
                 WHERE id = 'ovr-inherit-1'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "ovr-inherit-1");
        assert_text(&rows.rows[0][1], "Global");
        assert_text(&rows.rows[0][2], "global");
    }
);

simulation_test!(
    lix_entity_view_select_pushes_down_literal_lixcol_overrides,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_select_override_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, untracked\
                 ) VALUES \
                 ('match-global', 'lix_select_override_schema', 'inlang', 'global', 'inlang_sdk', '{\"id\":\"match-global\"}', NULL, '1', 1), \
                 ('mismatch-file', 'lix_select_override_schema', 'other', 'global', 'inlang_sdk', '{\"id\":\"mismatch-file\"}', NULL, '1', 1), \
                 ('mismatch-plugin', 'lix_select_override_schema', 'inlang', 'global', 'other_plugin', '{\"id\":\"mismatch-plugin\"}', NULL, '1', 1), \
                 ('mismatch-untracked', 'lix_select_override_schema', 'inlang', 'global', 'inlang_sdk', '{\"id\":\"mismatch-untracked\"}', NULL, '1', 0), \
                 ('mismatch-metadata', 'lix_select_override_schema', 'inlang', 'global', 'inlang_sdk', '{\"id\":\"mismatch-metadata\"}', '{\"k\":1}', '1', 1), \
                 ('match-main', 'lix_select_override_schema', 'inlang', 'main', 'inlang_sdk', '{\"id\":\"match-main\"}', NULL, '1', 1)",
                &[],
            )
            .await
            .unwrap();

        let base_rows = engine
            .execute(
                "SELECT id \
                 FROM lix_select_override_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(base_rows.rows.clone());
        assert_eq!(base_rows.rows.len(), 1);
        assert_text(&base_rows.rows[0][0], "match-global");

        let by_version_rows = engine
            .execute(
                "SELECT id, lixcol_version_id, lixcol_inherited_from_version_id \
                 FROM lix_select_override_schema_by_version \
                 ORDER BY id, lixcol_version_id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(by_version_rows.rows.clone());
        assert_eq!(by_version_rows.rows.len(), 3);
        let mut match_global_rows = Vec::new();
        let mut match_main_rows = Vec::new();
        for row in &by_version_rows.rows {
            let id = match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected id text, got {other:?}"),
            };
            if id == "match-global" {
                match_global_rows.push(row);
            } else if id == "match-main" {
                match_main_rows.push(row);
            }
        }

        assert_eq!(match_global_rows.len(), 2);
        assert_eq!(match_main_rows.len(), 1);

        let has_global_local = match_global_rows.iter().any(|row| {
            matches!((&row[1], &row[2]), (Value::Text(version), Value::Null) if version == "global")
        });
        assert!(has_global_local);

        let has_inherited = match_global_rows.iter().any(|row| {
            matches!((&row[1], &row[2]), (Value::Text(version), Value::Text(parent)) if version != "global" && !parent.is_empty())
        });
        assert!(has_inherited);

        assert_eq!(match_main_rows[0][2], Value::Null);
    }
);

simulation_test!(
    lix_entity_view_select_pushes_down_inherited_from_version_override,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();
        install_inherited_override_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_version (\
                 id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                 ) VALUES (\
                 'active-inherited', 'active-inherited', 'global', 0, 'commit-inherited', 'working-inherited'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'active-inherited'",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES \
                 ('inherited-match', 'lix_inherited_override_schema', 'lix', 'global', 'lix', '{\"id\":\"inherited-match\"}', '1', 0), \
                 ('inherited-mismatch', 'lix_inherited_override_schema', 'lix', 'active-inherited', 'lix', '{\"id\":\"inherited-mismatch\"}', '1', 0)",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT id, lixcol_inherited_from_version_id \
                 FROM lix_inherited_override_schema \
                 ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "inherited-match");
        assert_text(&rows.rows[0][1], "global");
    }
);
