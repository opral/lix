use crate::support;

use lix_engine::{LixError, Value};
use support::simulation_test::assert_boolean_like;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_missing_version_id_error(error: &LixError, version_id: &str) {
    let expected = format!("version with id '{version_id}' does not exist");
    assert!(
        error.description.contains(&expected),
        "unexpected error message: {}",
        error.description
    );
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
    untracked: bool,
) {
    let sql = format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
         ) VALUES (\
         '{entity_id}', 'test_state_schema', 'test-file', '{version_id}', 'lix', '{snapshot_content}', '1', {untracked}\
         )",
        entity_id = entity_id,
        version_id = version_id,
        snapshot_content = snapshot_content.replace('\'', "''"),
        untracked = if untracked { "true" } else { "false" },
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(lix_state_select_exposes_commit_id, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.unwrap();

    register_test_schema(&engine).await;
    engine.create_named_version("version-a").await.unwrap();
    engine
        .switch_version("version-a".to_string())
        .await
        .unwrap();
    insert_state_row(
        &engine,
        "entity-commit",
        "version-a",
        "{\"value\":\"A\"}",
        false,
    )
    .await;

    let rows = engine
        .execute(
            "SELECT commit_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-commit' \
                   AND file_id = 'test-file'",
            &[],
        )
        .await
        .unwrap();

    sim.assert_deterministic(rows.statements[0].rows.clone());
    assert_eq!(rows.statements[0].rows.len(), 1);
    let commit_id = match &rows.statements[0].rows[0][0] {
        Value::Text(value) => value,
        other => panic!("expected text commit_id in lix_state, got {other:?}"),
    };
    assert!(!commit_id.is_empty(), "expected non-empty commit_id");
});

simulation_test!(
    lix_state_select_scopes_to_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();

        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(&engine, "entity-a", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-b", "version-b", "{\"value\":\"B\"}", false).await;

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"A\"}");
    }
);

simulation_test!(
    lix_state_select_switches_with_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(&engine, "entity-a", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-b", "version-b", "{\"value\":\"B\"}", false).await;

        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();
        let first = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(first.statements[0].rows.clone());
        assert_eq!(first.statements[0].rows.len(), 1);
        assert_text(&first.statements[0].rows[0][0], "entity-a");

        engine
            .switch_version("version-b".to_string())
            .await
            .unwrap();
        let second = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(second.statements[0].rows.clone());
        assert_eq!(second.statements[0].rows.len(), 1);
        assert_text(&second.statements[0].rows[0][0], "entity-b");
    }
);

simulation_test!(
    lix_state_select_prioritizes_untracked_in_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "entity-x",
            "version-a",
            "{\"value\":\"tracked\"}",
            false,
        )
        .await;
        insert_state_row(
            &engine,
            "entity-x",
            "version-a",
            "{\"value\":\"untracked\"}",
            true,
        )
        .await;

        let rows = engine
            .execute(
                "SELECT snapshot_content, untracked \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-x'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"untracked\"}");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
    }
);

simulation_test!(
    lix_state_select_without_schema_key_filter,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(&engine, "entity-a", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-b", "version-b", "{\"value\":\"B\"}", false).await;

        let rows = engine
            .execute(
                "SELECT entity_id, schema_key \
                 FROM lix_state \
                 WHERE file_id = 'test-file' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-a");
        assert_text(&rows.statements[0].rows[0][1], "test_state_schema");
    }
);

simulation_test!(
    lix_state_select_rejects_version_id_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();
        insert_state_row(
            &engine,
            "entity-version-col",
            "version-a",
            "{\"value\":\"A\"}",
            false,
        )
        .await;

        let error = engine
            .execute(
                "SELECT version_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-version-col'",
                &[],
            )
            .await
            .expect_err("lix_state should reject version_id column");

        assert!(
            error.description.contains("does not expose version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_select_rejects_lixcol_version_id_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();
        insert_state_row(
            &engine,
            "entity-version-col-2",
            "version-a",
            "{\"value\":\"A\"}",
            false,
        )
        .await;

        let error = engine
            .execute(
                "SELECT lixcol_version_id \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-version-col-2'",
                &[],
            )
            .await
            .expect_err("lix_state should reject lixcol_version_id column");

        assert!(
            error.description.contains("does not expose version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_select_rejects_version_id_from_wrapped_subquery,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();
        insert_state_row(
            &engine,
            "entity-subquery",
            "version-a",
            "{\"value\":\"A\"}",
            false,
        )
        .await;

        let error = engine
            .execute(
                "SELECT 1 \
                 FROM (SELECT * FROM lix_state) s \
                 WHERE s.version_id IS NOT NULL",
                &[],
            )
            .await
            .expect_err("subquery access to version_id should fail");

        let description = error.description.to_ascii_lowercase();
        assert!(
            description.contains("version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_select_reflects_untracked_entity_after_vtable_update,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "untracked-entity",
            "version-a",
            "{\"value\":\"initial\"}",
            true,
        )
        .await;

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'untracked-entity' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a' \
                   AND untracked = true",
                &[],
            )
            .await
            .unwrap();

        let updated = engine
            .execute(
                "SELECT snapshot_content, untracked \
                 FROM lix_state \
                 WHERE entity_id = 'untracked-entity'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(updated.statements[0].rows.clone());
        assert_eq!(updated.statements[0].rows.len(), 1);
        assert_text(&updated.statements[0].rows[0][0], "{\"value\":\"updated\"}");
        assert_boolean_like(&updated.statements[0].rows[0][1], true);
    }
);

simulation_test!(
    lix_state_insert_routes_to_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();

        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
             entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'entity-0', 'file-0', 'test_state_schema', 'lix', '1', '{\"value\":\"A\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let first = engine
            .execute(
                "SELECT version_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-0' \
               AND file_id = 'file-0' \
             ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(first.statements[0].rows.clone());
        assert_eq!(first.statements[0].rows.len(), 1);
        assert_text(&first.statements[0].rows[0][0], "version-a");
        assert_text(&first.statements[0].rows[0][1], "{\"value\":\"A\"}");

        engine
            .switch_version("version-b".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
             entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'entity-0', 'file-0', 'test_state_schema', 'lix', '1', '{\"value\":\"B\"}'\
             )",
                &[],
            )
            .await
            .unwrap();

        let second = engine
            .execute(
                "SELECT version_id, snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-0' \
               AND file_id = 'file-0' \
             ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(second.statements[0].rows.clone());
        assert_eq!(second.statements[0].rows.len(), 2);
        assert_text(&second.statements[0].rows[0][0], "version-a");
        assert_text(&second.statements[0].rows[0][1], "{\"value\":\"A\"}");
        assert_text(&second.statements[0].rows[1][0], "version-b");
        assert_text(&second.statements[0].rows[1][1], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    lix_state_insert_routes_to_active_version_with_placeholders,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    Value::Text("entity-p".to_string()),
                    Value::Text("file-p".to_string()),
                    Value::Text("test_state_schema".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("1".to_string()),
                    Value::Text("{\"value\":\"P\"}".to_string()),
                ],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-p' \
                   AND file_id = 'file-p'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"P\"}");
    }
);

simulation_test!(
    lix_state_by_version_insert_rejects_missing_version_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;

        let error = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-missing-version', 'test_state_schema', 'test-file', 'version-missing', 'lix', '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await
            .expect_err("insert with missing version_id should fail");

        assert_missing_version_id_error(&error, "version-missing");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_update_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert', 'file-upsert', 'test_state_schema', 'lix', '1', '{\"value\":\"A\"}'\
                 )", &[])
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert', 'file-upsert', 'test_state_schema', 'lix', '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'", &[])
            .await
            .unwrap();

        let visible = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert' \
                   AND file_id = 'file-upsert'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(visible.statements[0].rows.clone());
        assert_eq!(visible.statements[0].rows.len(), 1);
        assert_text(&visible.statements[0].rows[0][0], "{\"value\":\"B\"}");

        let materialized = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert' \
                   AND file_id = 'file-upsert' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(materialized.statements[0].rows.clone());
        assert_eq!(materialized.statements[0].rows.len(), 1);
        assert_text(&materialized.statements[0].rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert', 'file-upsert', 'test_state_schema', 'lix', '1', '{\"value\":\"A\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert', 'file-upsert', 'test_state_schema', 'lix', '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id) DO NOTHING", &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert' \
                   AND file_id = 'file-upsert'",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"A\"}");
    }
);

simulation_test!(
    lix_state_insert_rejects_explicit_version_id_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        let error = engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-x', 'file-x', 'test_state_schema', 'version-b', 'lix', '1', '{\"value\":\"x\"}'\
                 )", &[])
            .await
            .expect_err("lix_state insert with version_id should fail");

        assert!(
            error
                .description
                .contains("lix_state insert cannot set version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_update_routes_to_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(
            &engine,
            "entity-u",
            "version-a",
            "{\"value\":\"A-initial\"}",
            false,
        )
        .await;
        insert_state_row(
            &engine,
            "entity-u",
            "version-b",
            "{\"value\":\"B-initial\"}",
            false,
        )
        .await;

        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"A-updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-u' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-u' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"A-updated\"}");
        assert_text(&rows.statements[0].rows[1][0], "version-b");
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"B-initial\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_rejects_missing_version_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;

        let error = engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"A-updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-missing-version' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-missing'",
                &[],
            )
            .await
            .expect_err("update with missing version_id should fail");

        assert_missing_version_id_error(&error, "version-missing");
    }
);

simulation_test!(
    lix_state_update_supports_placeholder_schema_key_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-placeholder-schema",
            "version-a",
            "{\"value\":\"before\"}",
            false,
        )
        .await;

        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = $1 \
                 WHERE file_id = $2 \
                   AND schema_key = $3 \
                   AND entity_id = $4",
                &[
                    Value::Text("{\"value\":\"after\"}".to_string()),
                    Value::Text("test-file".to_string()),
                    Value::Text("test_state_schema".to_string()),
                    Value::Text("entity-placeholder-schema".to_string()),
                ],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-placeholder-schema' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"after\"}");
    }
);

simulation_test!(
    lix_state_update_allows_untracked_with_untracked_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "untracked-entity-u",
            "version-a",
            "{\"value\":\"initial\"}",
            true,
        )
        .await;

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'untracked-entity-u' \
                   AND untracked = true",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content, untracked \
                 FROM lix_state \
                 WHERE entity_id = 'untracked-entity-u'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "{\"value\":\"updated\"}");
        assert_boolean_like(&rows.statements[0].rows[0][1], true);
    }
);

simulation_test!(
    lix_state_update_without_untracked_predicate_updates_effective_untracked_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "effective-entity-u",
            "version-a",
            "{\"value\":\"tracked-initial\"}",
            false,
        )
        .await;
        insert_state_row(
            &engine,
            "effective-entity-u",
            "version-a",
            "{\"value\":\"untracked-initial\"}",
            true,
        )
        .await;

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"effective-updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'effective-entity-u' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        let visible = engine
            .execute(
                "SELECT snapshot_content, untracked \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'effective-entity-u' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(visible.statements[0].rows.clone());
        assert_eq!(visible.statements[0].rows.len(), 1);
        assert_text(
            &visible.statements[0].rows[0][0],
            "{\"value\":\"effective-updated\"}",
        );
        assert_boolean_like(&visible.statements[0].rows[0][1], true);

        let tracked = engine
            .execute(
                "SELECT value \
                 FROM lix_internal_live_v1_test_state_schema \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'effective-entity-u' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a' \
                   AND untracked = false \
                   AND is_tombstone = 0",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(tracked.statements[0].rows.len(), 1);
        assert_text(&tracked.statements[0].rows[0][0], "tracked-initial");
    }
);

simulation_test!(
    lix_state_update_partitions_tracked_and_untracked_targets,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "mixed-tracked",
            "version-a",
            "{\"value\":\"tracked-before\"}",
            false,
        )
        .await;
        insert_state_row(
            &engine,
            "mixed-untracked",
            "version-a",
            "{\"value\":\"untracked-before\"}",
            true,
        )
        .await;

        let selector_rows = engine
            .execute(
                "SELECT entity_id, file_id, plugin_key, schema_version, global, untracked \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND file_id = 'test-file' \
                 ORDER BY entity_id ASC",
                &[],
            )
            .await
            .expect("mixed selector rows should be queryable before update");

        sim.assert_deterministic_normalized(selector_rows.statements[0].rows.clone());
        assert_eq!(selector_rows.statements[0].rows.len(), 2);

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .expect("mixed tracked/untracked update should succeed");

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content, untracked \
                 FROM lix_state \
                 WHERE entity_id IN ('mixed-tracked', 'mixed-untracked') \
                 ORDER BY entity_id ASC",
                &[],
            )
            .await
            .expect("updated mixed rows should be queryable");

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "mixed-tracked");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"after\"}");
        assert_boolean_like(&rows.statements[0].rows[0][2], false);
        assert_text(&rows.statements[0].rows[1][0], "mixed-untracked");
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"after\"}");
        assert_boolean_like(&rows.statements[0].rows[1][2], true);
    }
);

simulation_test!(
    lix_state_update_rejects_explicit_version_id_assignment,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-ver",
            "version-a",
            "{\"value\":\"before\"}",
            false,
        )
        .await;
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        let error = engine
            .execute(
                "UPDATE lix_state \
                 SET version_id = 'version-b' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-ver'",
                &[],
            )
            .await
            .expect_err("lix_state update with version_id assignment should fail");
        assert!(
            error
                .description
                .contains("lix_state update cannot set version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_update_rejects_version_id_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-ver-pred",
            "version-a",
            "{\"value\":\"before\"}",
            false,
        )
        .await;
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        let error = engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-ver-pred' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .expect_err("lix_state update with version_id predicate should fail");

        assert!(
            error.description.contains("does not expose version_id"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_update_rejects_unknown_assignment_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-unknown-col",
            "version-a",
            "{\"value\":\"before\"}",
            false,
        )
        .await;
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        let error = engine
            .execute(
                "UPDATE lix_state \
                 SET bogus = 'x' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-unknown-col'",
                &[],
            )
            .await
            .expect_err("lix_state update with unknown assignment should fail");
        assert!(
            error.description.contains("strict rewrite violation")
                && error.description.contains("unknown column")
                && error.description.contains("bogus"),
            "unexpected error message: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_delete_routes_to_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(
            &engine,
            "entity-d",
            "version-a",
            "{\"value\":\"A-initial\"}",
            false,
        )
        .await;
        insert_state_row(
            &engine,
            "entity-d",
            "version-b",
            "{\"value\":\"B-initial\"}",
            false,
        )
        .await;
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-d' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-d' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-b");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"B-initial\"}");

        let history_rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_change \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-d' \
                   AND file_id = 'test-file' \
                   AND snapshot_content IS NULL \
                 ORDER BY created_at DESC, id DESC",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(history_rows.statements[0].rows.clone());
        assert_eq!(history_rows.statements[0].rows.len(), 1);
        assert_eq!(history_rows.statements[0].rows[0][0], Value::Null);
    }
);

simulation_test!(
    lix_state_delete_allows_untracked_with_untracked_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        insert_state_row(
            &engine,
            "untracked-entity-d",
            "version-a",
            "{\"value\":\"A-untracked\"}",
            true,
        )
        .await;
        insert_state_row(
            &engine,
            "untracked-entity-d",
            "version-b",
            "{\"value\":\"B-untracked\"}",
            true,
        )
        .await;

        engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'untracked-entity-d' \
                   AND file_id = 'test-file' \
                   AND untracked = true",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content, untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'untracked-entity-d' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-b");
        assert_text(
            &rows.statements[0].rows[0][1],
            "{\"value\":\"B-untracked\"}",
        );
        assert_boolean_like(&rows.statements[0].rows[0][2], true);
    }
);

simulation_test!(
    lix_state_delete_rejects_version_id_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(&engine, "entity-s", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-s", "version-b", "{\"value\":\"B\"}", false).await;
        engine
            .switch_version("version-a".to_string())
            .await
            .unwrap();

        let error = engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-s' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-b'",
                &[],
            )
            .await
            .expect_err("lix_state delete with version_id predicate should fail");

        assert!(
            error.description.contains("does not expose version_id"),
            "unexpected error message: {}",
            error.description
        );

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-s' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"A\"}");
        assert_text(&rows.statements[0].rows[1][0], "version-b");
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"B\"}");
    }
);
