mod support;

use lix_engine::Value;
use support::simulation_test::assert_boolean_like;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"test_state_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .unwrap();
}

async fn insert_version(engine: &support::simulation_test::SimulationEngine, version_id: &str) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', 'global', false, 'commit-{version_id}', 'working-{version_id}'\
         )",
    );
    engine.execute(&sql, &[]).await.unwrap();
}

async fn insert_state_row(
    engine: &support::simulation_test::SimulationEngine,
    entity_id: &str,
    version_id: &str,
    snapshot_content: &str,
    untracked: bool,
) {
    let sql = format!(
        "INSERT INTO lix_internal_state_vtable (\
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
    engine.init().await.unwrap();

    register_test_schema(&engine).await;
    insert_version(&engine, "version-a").await;
    engine
        .execute(
            "UPDATE lix_active_version SET version_id = 'version-a'",
            &[],
        )
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

    sim.assert_deterministic(rows.rows.clone());
    assert_eq!(rows.rows.len(), 1);
    let commit_id = match &rows.rows[0][0] {
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
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "entity-a");
        assert_text(&rows.rows[0][1], "{\"value\":\"A\"}");
    }
);

simulation_test!(
    lix_state_select_switches_with_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
        insert_state_row(&engine, "entity-a", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-b", "version-b", "{\"value\":\"B\"}", false).await;

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
        sim.assert_deterministic(first.rows.clone());
        assert_eq!(first.rows.len(), 1);
        assert_text(&first.rows[0][0], "entity-a");

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-b'",
                &[],
            )
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
        sim.assert_deterministic(second.rows.clone());
        assert_eq!(second.rows.len(), 1);
        assert_text(&second.rows[0][0], "entity-b");
    }
);

simulation_test!(
    lix_state_select_prioritizes_untracked_in_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "{\"value\":\"untracked\"}");
        assert_boolean_like(&rows.rows[0][1], true);
    }
);

simulation_test!(
    lix_state_select_without_schema_key_filter,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "entity-a");
        assert_text(&rows.rows[0][1], "test_state_schema");
    }
);

simulation_test!(
    lix_state_select_reflects_untracked_entity_after_vtable_update,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                "UPDATE lix_internal_state_vtable \
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

        sim.assert_deterministic_normalized(updated.rows.clone());
        assert_eq!(updated.rows.len(), 1);
        assert_text(&updated.rows[0][0], "{\"value\":\"updated\"}");
        assert_boolean_like(&updated.rows[0][1], true);
    }
);

simulation_test!(
    lix_state_insert_routes_to_active_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-0' \
               AND file_id = 'file-0' \
             ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(first.rows.clone());
        assert_eq!(first.rows.len(), 1);
        assert_text(&first.rows[0][0], "version-a");
        assert_text(&first.rows[0][1], "{\"value\":\"A\"}");

        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-b'",
                &[],
            )
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
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-0' \
               AND file_id = 'file-0' \
             ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(second.rows.clone());
        assert_eq!(second.rows.len(), 2);
        assert_text(&second.rows[0][0], "version-a");
        assert_text(&second.rows[0][1], "{\"value\":\"A\"}");
        assert_text(&second.rows[1][0], "version-b");
        assert_text(&second.rows[1][1], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    lix_state_insert_routes_to_active_version_with_placeholders,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-p' \
                   AND file_id = 'file-p'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "version-a");
        assert_text(&rows.rows[0][1], "{\"value\":\"P\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_update_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                 ON CONFLICT (entity_id, schema_key, file_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'",
                &[],
            )
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

        sim.assert_deterministic(visible.rows.clone());
        assert_eq!(visible.rows.len(), 1);
        assert_text(&visible.rows[0][0], "{\"value\":\"B\"}");

        let materialized = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert' \
                   AND file_id = 'file-upsert' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(materialized.rows.clone());
        assert_eq!(materialized.rows.len(), 1);
        assert_text(&materialized.rows[0][0], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    lix_state_insert_on_conflict_do_nothing_is_rejected,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert', 'file-upsert', 'test_state_schema', 'lix', '1', '{\"value\":\"A\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id) DO NOTHING",
                &[],
            )
            .await
            .expect_err("DO NOTHING should be rejected");

        assert!(
            err.message
                .contains("ON CONFLICT DO NOTHING is not supported"),
            "unexpected error: {}",
            err.message
        );
    }
);

simulation_test!(
    lix_state_insert_rejects_explicit_version_id_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let error = engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-x', 'file-x', 'test_state_schema', 'version-b', 'lix', '1', '{\"value\":\"x\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("lix_state insert with version_id should fail");

        assert!(
            error
                .message
                .contains("lix_state insert cannot set version_id"),
            "unexpected error message: {}",
            error.message
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
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
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
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-u' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "version-a");
        assert_text(&rows.rows[0][1], "{\"value\":\"A-updated\"}");
        assert_text(&rows.rows[1][0], "version-b");
        assert_text(&rows.rows[1][1], "{\"value\":\"B-initial\"}");
    }
);

simulation_test!(
    lix_state_update_allows_untracked_with_untracked_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "{\"value\":\"updated\"}");
        assert_boolean_like(&rows.rows[0][1], true);
    }
);

simulation_test!(
    lix_state_update_rejects_explicit_version_id_assignment,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_state_row(
            &engine,
            "entity-ver",
            "version-a",
            "{\"value\":\"before\"}",
            false,
        )
        .await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                .message
                .contains("lix_state update cannot set version_id"),
            "unexpected error message: {}",
            error.message
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
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
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
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-d' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "version-a");
        assert_eq!(rows.rows[0][1], Value::Null);
        assert_text(&rows.rows[1][0], "version-b");
        assert_text(&rows.rows[1][1], "{\"value\":\"B-initial\"}");
    }
);

simulation_test!(
    lix_state_delete_allows_untracked_with_untracked_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
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
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'untracked-entity-d' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "version-b");
        assert_text(&rows.rows[0][1], "{\"value\":\"B-untracked\"}");
        assert_boolean_like(&rows.rows[0][2], true);
    }
);

simulation_test!(
    lix_state_delete_is_scoped_to_active_version_even_with_explicit_version_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;
        insert_state_row(&engine, "entity-s", "version-a", "{\"value\":\"A\"}", false).await;
        insert_state_row(&engine, "entity-s", "version-b", "{\"value\":\"B\"}", false).await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-s' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-b'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-s' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic_normalized(rows.rows.clone());
        assert_eq!(rows.rows.len(), 2);
        assert_text(&rows.rows[0][0], "version-a");
        assert_text(&rows.rows[0][1], "{\"value\":\"A\"}");
        assert_text(&rows.rows[1][0], "version-b");
        assert_text(&rows.rows[1][1], "{\"value\":\"B\"}");
    }
);
