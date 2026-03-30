use crate::support;

use lix_engine::Value;
use support::simulation_test::assert_boolean_like;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn normalize_bool_like_rows(rows: &[Vec<Value>], columns: &[usize]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, value)| {
                    if columns.contains(&index) {
                        match value {
                            Value::Boolean(actual) => Value::Boolean(*actual),
                            Value::Integer(actual) => Value::Boolean(*actual != 0),
                            Value::Text(actual) => Value::Boolean(matches!(
                                actual.trim().to_ascii_lowercase().as_str(),
                                "1" | "true"
                            )),
                            other => panic!("expected boolean-like value, got {other:?}"),
                        }
                    } else {
                        value.clone()
                    }
                })
                .collect()
        })
        .collect()
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
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

async fn insert_state_row(
    engine: &support::simulation_test::SimulationEngine,
    entity_id: &str,
    version_id: &str,
    snapshot_content: &str,
) {
    let sql = format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', 'test_state_schema', 'test-file', '{version_id}', 'lix', '{snapshot_content}', '1'\
         )",
        entity_id = entity_id,
        version_id = version_id,
        snapshot_content = snapshot_content.replace('\'', "''"),
    );
    engine.execute(&sql, &[]).await.unwrap();
}

// TODO(m24): Add `_by_version` inheritance coverage here too, mirroring
// `lix_state` expectations for explicit version scopes.

simulation_test!(
    lix_state_select_inherits_from_parent_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
        engine
            .switch_version("version-child".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "entity-inherited",
            "global",
            "{\"value\":\"global\"}",
        )
        .await;

        let rows = engine
            .execute(
                "SELECT entity_id, global, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-inherited'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[1]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-inherited");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
        assert_text(&rows.statements[0].rows[0][2], "{\"value\":\"global\"}");
    }
);

simulation_test!(
    lix_state_select_prefers_child_row_over_parent,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
        engine
            .switch_version("version-child".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "entity-override",
            "global",
            "{\"value\":\"global\"}",
        )
        .await;
        insert_state_row(
            &engine,
            "entity-override",
            "version-child",
            "{\"value\":\"child\"}",
        )
        .await;

        let rows = engine
            .execute(
                "SELECT global, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-override'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[0]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_boolean_like(&rows.statements[0].rows[0][0], false);
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"child\"}");
    }
);

simulation_test!(
    lix_state_select_child_tombstone_hides_parent_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
        engine
            .switch_version("version-child".to_string())
            .await
            .unwrap();

        insert_state_row(&engine, "entity-tomb", "global", "{\"value\":\"global\"}").await;
        insert_state_row(
            &engine,
            "entity-tomb",
            "version-child",
            "{\"value\":\"child\"}",
        )
        .await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-tomb' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-tomb'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[1]));
        assert!(rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_state_delete_with_inherited_null_filter_deletes_only_local_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
        engine
            .switch_version("version-child".to_string())
            .await
            .unwrap();

        insert_state_row(&engine, "entity-global", "global", "{\"value\":\"global\"}").await;
        insert_state_row(
            &engine,
            "entity-local",
            "version-child",
            "{\"value\":\"local\"}",
        )
        .await;

        engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id LIKE 'entity-%' \
                   AND global = false",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, global, snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[1]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-global");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
        assert_text(&rows.statements[0].rows[0][2], "{\"value\":\"global\"}");
    }
);
