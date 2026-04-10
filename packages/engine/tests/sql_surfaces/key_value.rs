use crate::support;

use std::collections::HashMap;

use lix_engine::{BootKeyValue, Value};
use serde_json::json;

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

simulation_test!(
    key_value_crud_is_handled_through_state_surface,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(&insert_key_value_sql("key0", "\"value0\""), &[])
            .await
            .unwrap();

        let after_insert = engine
            .execute(
                "SELECT snapshot_content, untracked \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(vec![vec![after_insert.statements[0].rows[0][0].clone()]]);
        assert_eq!(after_insert.statements[0].rows.len(), 1);
        assert_eq!(
            after_insert.statements[0].rows[0][0],
            Value::Text("{\"key\":\"key0\",\"value\":\"value0\"}".to_string())
        );
        match &after_insert.statements[0].rows[0][1] {
            Value::Boolean(value) => assert!(!value),
            Value::Integer(value) => assert_eq!(*value, 0),
            other => panic!("expected false-like untracked marker, got {other:?}"),
        }

        engine
            .execute(
                "UPDATE lix_state_by_version \
             SET snapshot_content = '{\"key\":\"key0\",\"value\":\"value1\"}' \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0' AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();

        let after_update = engine
            .execute(
                "SELECT snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(after_update.statements[0].rows.clone());
        assert_eq!(after_update.statements[0].rows.len(), 1);
        assert_eq!(
            after_update.statements[0].rows[0][0],
            Value::Text("{\"key\":\"key0\",\"value\":\"value1\"}".to_string())
        );

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0' AND version_id = 'global'",
                &[],
            )
            .await
            .unwrap();

        let after_delete = engine
            .execute(
                "SELECT entity_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'key0' \
               AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(after_delete.statements[0].rows.clone());
        assert_eq!(after_delete.statements[0].rows.len(), 0);
    }
);

simulation_test!(
    boot_key_values_default_to_active_version_scope,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(Some(support::simulation_test::SimulatedLixBootArgs {
                key_values: vec![BootKeyValue {
                    key: "boot-default-active".to_string(),
                    value: json!("active-value"),
                    lixcol_global: None,
                    lixcol_untracked: Some(false),
                }],
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let active_version_id = engine.active_version_id().await.unwrap();

        let result = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'boot-default-active' \
                   AND version_id = $1 \
                   AND global = false \
                   AND snapshot_content IS NOT NULL",
                &[Value::Text(active_version_id)],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows, vec![vec![Value::Integer(1)]]);
    }
);

simulation_test!(
    boot_key_values_can_target_global_scope,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(Some(support::simulation_test::SimulatedLixBootArgs {
                key_values: vec![BootKeyValue {
                    key: "boot-global".to_string(),
                    value: json!("global-value"),
                    lixcol_global: Some(true),
                    lixcol_untracked: Some(false),
                }],
                ..Default::default()
            }))
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'boot-global' \
                   AND version_id = 'global' \
                   AND global = true \
                   AND snapshot_content IS NOT NULL",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(result.statements[0].rows, vec![vec![Value::Integer(1)]]);
    }
);

simulation_test!(
    key_value_update_with_sparse_placeholders_routes_through_public_lowering,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('placeholder-key', 'before')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        engine
            .execute(
                "UPDATE lix_key_value SET value = ?2 WHERE key = ?1",
                &[
                    Value::Text("placeholder-key".to_string()),
                    Value::Text("after".to_string()),
                ],
            )
            .await
            .expect("sparse placeholder update should succeed");

        let updated = engine
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'placeholder-key'",
                &[],
            )
            .await
            .expect("verification query should succeed");

        sim.assert_deterministic(updated.statements[0].rows.clone());
        assert_eq!(
            updated.statements[0].rows,
            vec![vec![Value::Text("after".to_string())]]
        );
    }
);

simulation_test!(key_value_allows_arbitrary_json_values, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");

    engine.initialize().await.unwrap();

    let fixtures = [
        ("key0", "{\"foo\":\"bar\"}"),
        ("key1", "[\"foo\",\"bar\"]"),
        ("key2", "\"foo\""),
        ("key3", "42"),
        ("key4", "true"),
        ("key5", "null"),
    ];

    for (key, value_json) in fixtures {
        engine
            .execute(&insert_key_value_sql(key, value_json), &[])
            .await
            .unwrap();
    }

    let rows = engine
        .execute(
            "SELECT entity_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id IN ('key0', 'key1', 'key2', 'key3', 'key4', 'key5') \
             ORDER BY entity_id",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(rows.statements[0].rows.clone());
    assert_eq!(rows.statements[0].rows.len(), 6);

    let actual_by_id: HashMap<String, String> = rows.statements[0]
        .rows
        .iter()
        .map(|row| {
            let entity_id = match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected text entity_id, got {other:?}"),
            };
            let snapshot_content = match &row[1] {
                Value::Text(value) => value.clone(),
                other => panic!("expected text snapshot_content, got {other:?}"),
            };
            (entity_id, snapshot_content)
        })
        .collect();

    assert_eq!(
        actual_by_id.get("key0"),
        Some(&"{\"key\":\"key0\",\"value\":{\"foo\":\"bar\"}}".to_string())
    );
    assert_eq!(
        actual_by_id.get("key1"),
        Some(&"{\"key\":\"key1\",\"value\":[\"foo\",\"bar\"]}".to_string())
    );
    assert_eq!(
        actual_by_id.get("key2"),
        Some(&"{\"key\":\"key2\",\"value\":\"foo\"}".to_string())
    );
    assert_eq!(
        actual_by_id.get("key3"),
        Some(&"{\"key\":\"key3\",\"value\":42}".to_string())
    );
    assert_eq!(
        actual_by_id.get("key4"),
        Some(&"{\"key\":\"key4\",\"value\":true}".to_string())
    );
    assert_eq!(
        actual_by_id.get("key5"),
        Some(&"{\"key\":\"key5\",\"value\":null}".to_string())
    );
});

simulation_test!(
    key_value_distinguishes_string_and_number_literals,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(&insert_key_value_sql("type_test_string", "\"1\""), &[])
            .await
            .unwrap();
        engine
            .execute(&insert_key_value_sql("type_test_number", "1"), &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id IN ('type_test_string', 'type_test_number') \
             ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);

        let number_row = &rows.statements[0].rows[0];
        let string_row = &rows.statements[0].rows[1];

        assert_eq!(number_row[0], Value::Text("type_test_number".to_string()));
        assert_eq!(
            number_row[1],
            Value::Text("{\"key\":\"type_test_number\",\"value\":1}".to_string())
        );

        assert_eq!(string_row[0], Value::Text("type_test_string".to_string()));
        assert_eq!(
            string_row[1],
            Value::Text("{\"key\":\"type_test_string\",\"value\":\"1\"}".to_string())
        );
    }
);

simulation_test!(
    key_value_lix_json_extract_supports_array_index_segments,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(&insert_key_value_sql("array_extract", "[10,20,30]"), &[])
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'value', 1) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' AND entity_id = 'array_extract' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(result.statements[0].rows.len(), 1);
        assert_eq!(
            result.statements[0].rows[0][0],
            Value::Text("20".to_string())
        );
    }
);

simulation_test!(
    key_value_lix_json_extract_supports_numeric_object_key_segments,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql("numeric_key_extract", "{\"1\":\"one\"}"),
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'value', '1') \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' AND entity_id = 'numeric_key_extract' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(
            result.statements[0].rows,
            vec![vec![Value::Text("one".to_string())]]
        );
    }
);

simulation_test!(
    key_value_lix_json_extract_supports_empty_object_key_segments,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                &insert_key_value_sql("empty_key_extract", "{\"\":\"blank\"}"),
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'value', '') \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' AND entity_id = 'empty_key_extract' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(
            result.statements[0].rows,
            vec![vec![Value::Text("blank".to_string())]]
        );
    }
);

simulation_test!(
    key_value_lix_json_extract_returns_json_boolean_lexemes,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT \
                    lix_json_extract('{\"value\":true}', 'value'), \
                    lix_json_extract('{\"value\":false}', 'value'), \
                    lix_json_extract_boolean('{\"value\":true}', 'value'), \
                    lix_json_extract_boolean('{\"value\":\"true\"}', 'value')",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(result.statements[0].rows.clone());
        assert_eq!(
            result.statements[0].rows[0][0],
            Value::Text("true".to_string())
        );
        assert_eq!(
            result.statements[0].rows[0][1],
            Value::Text("false".to_string())
        );
        support::simulation_test::assert_boolean_like(&result.statements[0].rows[0][2], true);
        assert_eq!(result.statements[0].rows[0][3], Value::Null);
    }
);

simulation_test!(
    key_value_lix_json_normalizes_literal_scalars,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let result = engine
            .execute(
                "SELECT \
                CAST(lix_json(true) AS TEXT), \
                CAST(lix_json(false) AS TEXT), \
                CAST(lix_json(NULL) AS TEXT), \
                CAST(lix_json(1) AS TEXT)",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.statements[0].rows.clone());
        assert_eq!(
            result.statements[0].rows,
            vec![vec![
                Value::Text("true".to_string()),
                Value::Text("false".to_string()),
                Value::Text("null".to_string()),
                Value::Text("1".to_string()),
            ]]
        );
    }
);
