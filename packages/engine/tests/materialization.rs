mod support;

use std::collections::BTreeSet;

use lix_engine::{
    BootKeyValue, MaterializationDebugMode, MaterializationRequest, MaterializationScope, Value,
};

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"materialization_test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn main_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT id FROM lix_version WHERE name = 'main' LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected main version id text, got {other:?}"),
    }
}

simulation_test!(
    materialization_plan_exposes_debug_trace_and_is_deterministic,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: vec![BootKeyValue {
                    key: "lix_deterministic_mode".to_string(),
                    value: serde_json::json!({ "enabled": true }),
                    version_id: None,
                }],
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'entity-1', 'materialization_test_schema', 'file-1', '{}', 'lix', '{{\"value\":\"A\"}}', '1'\
                     )",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();

        let plan = engine
            .materialization_plan(&MaterializationRequest {
                scope: MaterializationScope::Full,
                debug: MaterializationDebugMode::Full,
                debug_row_limit: 256,
            })
            .await
            .unwrap();

        assert!(!plan.writes.is_empty(), "expected materialization writes");
        let schema_keys: BTreeSet<String> = plan
            .writes
            .iter()
            .map(|write| write.schema_key.clone())
            .collect();
        assert!(schema_keys.contains("lix_commit"));
        assert!(schema_keys.contains("lix_version_pointer"));
        assert!(schema_keys.contains("lix_change_set_element"));
        assert!(schema_keys.contains("lix_commit_edge"));

        let debug = plan.debug.as_ref().expect("expected debug trace");
        assert!(!debug.tips_by_version.is_empty(), "expected version tips");
        assert!(
            !debug.traversed_commits.is_empty(),
            "expected traversed commits"
        );

        let deterministic_payload = serde_json::to_string(&plan).unwrap();
        sim.assert_deterministic(deterministic_payload);
    }
);

simulation_test!(apply_materialization_plan_upserts_rows, |sim| async move {
    let engine = sim
        .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
            key_values: vec![BootKeyValue {
                key: "lix_deterministic_mode".to_string(),
                value: serde_json::json!({ "enabled": true }),
                version_id: None,
            }],
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.unwrap();

    register_test_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;

    engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'entity-2', 'materialization_test_schema', 'file-1', '{}', 'lix', '{{\"value\":\"B\"}}', '1'\
                     )",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();

    let plan = engine
        .materialization_plan(&MaterializationRequest {
            scope: MaterializationScope::Full,
            debug: MaterializationDebugMode::Summary,
            debug_row_limit: 128,
        })
        .await
        .unwrap();

    let first_report = engine.apply_materialization_plan(&plan).await.unwrap();

    assert_eq!(first_report.rows_written, plan.writes.len());
    assert!(first_report.rows_written > 0);
    sim.assert_deterministic(first_report.rows_written as i64);

    let next_plan = engine
        .materialization_plan(&MaterializationRequest {
            scope: MaterializationScope::Full,
            debug: MaterializationDebugMode::Summary,
            debug_row_limit: 128,
        })
        .await
        .unwrap();
    let report = engine.apply_materialization_plan(&next_plan).await.unwrap();
    assert!(report.rows_deleted > 0);
    assert!(report.rows_written > 0);
    sim.assert_deterministic(vec![report.rows_written as i64, report.rows_deleted as i64]);

    let rows = engine
        .execute(
            &format!(
                "SELECT snapshot_content, change_id \
                     FROM lix_internal_state_vtable \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-2' \
                       AND version_id = '{}' \
                       AND snapshot_content IS NOT NULL \
                     LIMIT 1",
                main_version_id
            ),
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(text) => assert_eq!(text, "{\"value\":\"B\"}"),
        other => panic!("expected text snapshot_content, got {other:?}"),
    }
    match &rows.rows[0][1] {
        Value::Text(text) => assert!(!text.is_empty()),
        other => panic!("expected text change_id, got {other:?}"),
    }

    sim.assert_deterministic(rows.rows.clone());
});
