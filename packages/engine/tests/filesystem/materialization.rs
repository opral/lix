use crate::support;

use std::collections::BTreeSet;

use lix_engine::{
    CanonicalJson, ExecuteOptions, LiveStateRebuildDebugMode, LiveStateRebuildPlan,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateWrite, LiveStateWriteOp, Value,
};

fn scrub_timestamp_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if key == "created_at" || key == "updated_at" || key == "timestamp" {
                    *entry = serde_json::Value::String("__timestamp__".to_string());
                } else {
                    scrub_timestamp_fields(entry);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                scrub_timestamp_fields(item);
            }
        }
        _ => {}
    }
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .register_schema(
            &serde_json::from_str::<serde_json::Value>(
                "{\"x-lix-key\":\"materialization_test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}",
            )
            .unwrap(),
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
    assert_eq!(rows.statements[0].rows.len(), 1);
    match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected main version id text, got {other:?}"),
    }
}

simulation_test!(
    materialization_plan_exposes_debug_trace_and_is_deterministic,
    simulations = [sqlite, postgres, materialization, timestamp_shuffle],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

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
                ), &[])
            .await
            .unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'entity-1b', 'materialization_test_schema', 'file-1', '{}', 'lix', '{{\"value\":\"B\"}}', '1'\
                     )",
                    main_version_id
                ), &[])
            .await
            .unwrap();

        let plan = engine
            .live_state_rebuild_plan(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Full,
                debug_row_limit: 256,
            })
            .await
            .unwrap();

        assert!(!plan.writes.is_empty(), "expected materialization writes");
        let schema_keys: BTreeSet<String> = plan
            .writes
            .iter()
            .map(|write| write.schema_key.to_string())
            .collect();
        assert!(schema_keys.contains("lix_commit"));
        assert!(schema_keys.contains("lix_change_set_element"));
        assert!(schema_keys.contains("lix_commit_edge"));

        let debug = plan.debug.as_ref().expect("expected debug trace");
        assert!(!debug.heads_by_version.is_empty(), "expected version heads");
        assert!(
            debug
                .heads_by_version
                .iter()
                .any(|head| head.version_id.as_str() == main_version_id),
            "expected main version head in debug trace"
        );
        assert!(
            !debug.traversed_commits.is_empty(),
            "expected traversed commits"
        );

        let mut deterministic_payload = serde_json::to_value(&plan).unwrap();
        scrub_timestamp_fields(&mut deterministic_payload);
        let deterministic_payload = serde_json::to_string(&deterministic_payload).unwrap();
        sim.assert_deterministic(deterministic_payload);
    }
);

simulation_test!(
    apply_materialization_plan_upserts_rows,
    simulations = [sqlite, postgres, materialization, timestamp_shuffle],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

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
                    ), &[])
                .await
                .unwrap();

        let plan = engine
            .live_state_rebuild_plan(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Summary,
                debug_row_limit: 128,
            })
            .await
            .unwrap();

        let first_report = engine.apply_live_state_rebuild_plan(&plan).await.unwrap();

        assert_eq!(first_report.rows_written, plan.writes.len());
        assert!(first_report.rows_written > 0);
        sim.assert_deterministic(first_report.rows_written as i64);

        let next_plan = engine
            .live_state_rebuild_plan(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Summary,
                debug_row_limit: 128,
            })
            .await
            .unwrap();
        let report = engine
            .apply_live_state_rebuild_plan(&next_plan)
            .await
            .unwrap();
        assert!(report.rows_deleted > 0);
        assert!(report.rows_written > 0);
        sim.assert_deterministic(vec![report.rows_written as i64, report.rows_deleted as i64]);

        let rows = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, change_id \
                         FROM lix_state_by_version \
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

        assert_eq!(rows.statements[0].rows.len(), 1);
        match &rows.statements[0].rows[0][0] {
            Value::Text(text) => assert_eq!(text, "{\"value\":\"B\"}"),
            other => panic!("expected text snapshot_content, got {other:?}"),
        }
        match &rows.statements[0].rows[0][1] {
            Value::Text(text) => assert!(!text.is_empty()),
            other => panic!("expected text change_id, got {other:?}"),
        }

        sim.assert_deterministic(rows.statements[0].rows.clone());
    }
);

simulation_test!(
    apply_materialization_plan_full_scope_clears_existing_rows_in_schema_tables,
    simulations = [sqlite, postgres, timestamp_shuffle],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;

        let scoped_versions = BTreeSet::from([main_version_id.clone()]);
        let seed_plan = LiveStateRebuildPlan {
            run_id: "seed".to_string(),
            scope: LiveStateRebuildScope::Versions(scoped_versions.clone()),
            stats: Vec::new(),
            writes: vec![LiveStateWrite {
                schema_key: "materialization_test_schema".try_into().unwrap(),
                entity_id: "entity-old".try_into().unwrap(),
                file_id: "file-1".try_into().unwrap(),
                version_id: main_version_id.clone().try_into().unwrap(),
                global: false,
                op: LiveStateWriteOp::Upsert,
                snapshot_content: Some(
                    CanonicalJson::from_text("{\"value\":\"old\"}")
                        .expect("test payload should be valid canonical json"),
                ),
                metadata: None,
                schema_version: "1".try_into().unwrap(),
                plugin_key: "lix".try_into().unwrap(),
                change_id: "seed-change".to_string(),
                created_at: "1970-01-01T00:00:00Z".to_string(),
                updated_at: "1970-01-01T00:00:00Z".to_string(),
            }],
            warnings: Vec::new(),
            debug: None,
        };
        engine
            .apply_live_state_rebuild_plan(&seed_plan)
            .await
            .unwrap();

        let full_plan = LiveStateRebuildPlan {
            run_id: "full".to_string(),
            scope: LiveStateRebuildScope::Full,
            stats: Vec::new(),
            writes: vec![LiveStateWrite {
                schema_key: "materialization_test_schema".try_into().unwrap(),
                entity_id: "entity-new".try_into().unwrap(),
                file_id: "file-1".try_into().unwrap(),
                version_id: main_version_id.clone().try_into().unwrap(),
                global: false,
                op: LiveStateWriteOp::Upsert,
                snapshot_content: Some(
                    CanonicalJson::from_text("{\"value\":\"new\"}")
                        .expect("test payload should be valid canonical json"),
                ),
                metadata: None,
                schema_version: "1".try_into().unwrap(),
                plugin_key: "lix".try_into().unwrap(),
                change_id: "full-change".to_string(),
                created_at: "1970-01-01T00:00:00Z".to_string(),
                updated_at: "1970-01-01T00:00:00Z".to_string(),
            }],
            warnings: Vec::new(),
            debug: None,
        };
        let report = engine
            .apply_live_state_rebuild_plan(&full_plan)
            .await
            .unwrap();

        assert!(report.rows_deleted > 0);
        assert_eq!(report.rows_written, 1);
        sim.assert_deterministic(vec![report.rows_written as i64, report.rows_deleted as i64]);

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'materialization_test_schema' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_eq!(
            rows.statements[0].rows[0][0],
            Value::Text("entity-new".to_string())
        );
        assert_eq!(
            rows.statements[0].rows[0][1],
            Value::Text("{\"value\":\"new\"}".to_string())
        );
        sim.assert_deterministic(rows.statements[0].rows.clone());
    }
);

simulation_test!(
    full_rebuild_keeps_semantic_state_without_preserving_writer_key,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;

        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'entity-writer', 'materialization_test_schema', 'file-1', '{}', 'lix', '{{\"value\":\"seed\"}}', '1'\
                     )",
                    main_version_id
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:seed".to_string()),
                },
            )
            .await
            .unwrap();

        let before = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before.statements[0].rows.len(), 1);
        assert_eq!(
            before.statements[0].rows[0][0],
            Value::Text("{\"value\":\"seed\"}".to_string())
        );
        assert_eq!(
            before.statements[0].rows[0][1],
            Value::Text("editor:seed".to_string())
        );

        engine
            .rebuild_live_state(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .expect("full rebuild should succeed without annotation hints");

        let raw_after = engine
            .execute(
                &format!(
                    "SELECT writer_key, change_id \
                     FROM lix_internal_live_v1_materialization_test_schema \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer' \
                       AND version_id = '{}' \
                       AND is_tombstone = 0 \
                       AND untracked = false",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(raw_after.statements[0].rows.len(), 1);
        assert_eq!(raw_after.statements[0].rows[0][0], Value::Null);
        match &raw_after.statements[0].rows[0][1] {
            Value::Text(change_id) => assert!(!change_id.is_empty()),
            other => panic!("expected text change_id after rebuild, got {other:?}"),
        }

        let after = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, writer_key, change_id \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after.statements[0].rows.len(), 1);
        assert_eq!(
            after.statements[0].rows[0][0],
            Value::Text("{\"value\":\"seed\"}".to_string())
        );
        assert_eq!(
            after.statements[0].rows[0][1],
            Value::Text("editor:seed".to_string())
        );
        match &after.statements[0].rows[0][2] {
            Value::Text(change_id) => assert!(!change_id.is_empty()),
            other => panic!("expected text change_id after rebuild, got {other:?}"),
        }

        let filtered = engine
            .execute(
                &format!(
                    "SELECT entity_id \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND version_id = '{}' \
                       AND writer_key = 'editor:seed'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(filtered.statements[0].rows.len(), 1);
        assert_eq!(
            filtered.statements[0].rows[0][0],
            Value::Text("entity-writer".to_string())
        );
    }
);

simulation_test!(
    full_rebuild_keeps_semantic_state_when_writer_key_annotation_is_missing,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;

        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'entity-writer-missing', 'materialization_test_schema', 'file-1', '{}', 'lix', '{{\"value\":\"seed\"}}', '1'\
                     )",
                    main_version_id
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:seed".to_string()),
                },
            )
            .await
            .unwrap();

        let raw_before = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_live_v1_materialization_test_schema \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer-missing' \
                       AND version_id = '{}' \
                       AND is_tombstone = 0 \
                       AND untracked = false",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(raw_before.statements[0].rows.len(), 1);
        assert_eq!(
            raw_before.statements[0].rows[0][0],
            Value::Text("editor:seed".to_string())
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_writer_key \
                     WHERE version_id = '{}' \
                       AND schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer-missing' \
                       AND file_id = 'file-1'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();

        let before_rebuild = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer-missing' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(before_rebuild.statements[0].rows.len(), 1);
        assert_eq!(
            before_rebuild.statements[0].rows[0][0],
            Value::Text("{\"value\":\"seed\"}".to_string())
        );
        assert_eq!(before_rebuild.statements[0].rows[0][1], Value::Null);

        engine
            .rebuild_live_state(&LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .expect("full rebuild should succeed without workspace writer annotation");

        let raw_after = engine
            .execute(
                &format!(
                    "SELECT writer_key, change_id \
                     FROM lix_internal_live_v1_materialization_test_schema \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer-missing' \
                       AND version_id = '{}' \
                       AND is_tombstone = 0 \
                       AND untracked = false",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(raw_after.statements[0].rows.len(), 1);
        assert_eq!(raw_after.statements[0].rows[0][0], Value::Null);
        match &raw_after.statements[0].rows[0][1] {
            Value::Text(change_id) => assert!(!change_id.is_empty()),
            other => panic!("expected text change_id after rebuild, got {other:?}"),
        }

        let after = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, writer_key, change_id \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND entity_id = 'entity-writer-missing' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(after.statements[0].rows.len(), 1);
        assert_eq!(
            after.statements[0].rows[0][0],
            Value::Text("{\"value\":\"seed\"}".to_string())
        );
        assert_eq!(after.statements[0].rows[0][1], Value::Null);
        match &after.statements[0].rows[0][2] {
            Value::Text(change_id) => assert!(!change_id.is_empty()),
            other => panic!("expected text change_id after rebuild, got {other:?}"),
        }

        let filtered = engine
            .execute(
                &format!(
                    "SELECT entity_id \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'materialization_test_schema' \
                       AND version_id = '{}' \
                       AND writer_key = 'editor:seed'",
                    main_version_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert!(
            filtered.statements[0].rows.is_empty(),
            "writer_key filtering should reflect missing workspace annotation state"
        );
    }
);
