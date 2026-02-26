mod support;

use std::collections::HashMap;

use lix_engine::Value;

fn insert_key_value_sql(key: &str, value_json: &str) -> String {
    format!(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{key}', 'lix_key_value', 'lix', 'global', 'lix', '{{\"key\":\"{key}\",\"value\":{value_json}}}', '1'\
         )"
    )
}

simulation_test!(key_value_crud_is_handled_through_vtable, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

    engine
        .execute(&insert_key_value_sql("key0", "\"value0\""), &[])
        .await
        .unwrap();

    let after_insert = engine
        .execute(
            "SELECT snapshot_content, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0'",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(vec![vec![after_insert.rows[0][0].clone()]]);
    assert_eq!(after_insert.rows.len(), 1);
    assert_eq!(
        after_insert.rows[0][0],
        Value::Text("{\"key\":\"key0\",\"value\":\"value0\"}".to_string())
    );
    match &after_insert.rows[0][1] {
        Value::Boolean(value) => assert!(!value),
        Value::Integer(value) => assert_eq!(*value, 0),
        other => panic!("expected false-like untracked marker, got {other:?}"),
    }

    engine
        .execute(
            "UPDATE lix_internal_state_vtable \
             SET snapshot_content = '{\"key\":\"key0\",\"value\":\"value1\"}' \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0' AND version_id = 'global'",
            &[],
        )
        .await
        .unwrap();

    let after_update = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0'",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(after_update.rows.clone());
    assert_eq!(after_update.rows.len(), 1);
    assert_eq!(
        after_update.rows[0][0],
        Value::Text("{\"key\":\"key0\",\"value\":\"value1\"}".to_string())
    );

    engine
        .execute(
            "DELETE FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'key0' AND version_id = 'global'",
            &[],
        )
        .await
        .unwrap();

    let after_delete = engine
        .execute(
            "SELECT entity_id \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id = 'key0' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(after_delete.rows.clone());
    assert_eq!(after_delete.rows.len(), 0);
});

simulation_test!(key_value_allows_arbitrary_json_values, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");

    engine.init().await.unwrap();

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
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id IN ('key0', 'key1', 'key2', 'key3', 'key4', 'key5') \
             ORDER BY entity_id",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(rows.rows.clone());
    assert_eq!(rows.rows.len(), 6);

    let actual_by_id: HashMap<String, String> = rows
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

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
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_key_value' \
               AND entity_id IN ('type_test_string', 'type_test_number') \
             ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 2);

        let number_row = &rows.rows[0];
        let string_row = &rows.rows[1];

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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.unwrap();

        engine
            .execute(&insert_key_value_sql("array_extract", "[10,20,30]"), &[])
            .await
            .unwrap();

        let result = engine
            .execute(
                "SELECT lix_json_extract(snapshot_content, 'value', '1') \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_key_value' AND entity_id = 'array_extract' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(result.rows.clone());
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("20".to_string()));
    }
);
