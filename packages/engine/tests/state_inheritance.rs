mod support;

use lix_engine::Value;

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
         '{version_id}', '{version_id}', 'global', 0, 'commit-{version_id}', 'working-{version_id}'\
         )",
    );
    engine.execute(&sql, &[]).await.unwrap();
}

async fn insert_state_row(
    engine: &support::simulation_test::SimulationEngine,
    entity_id: &str,
    version_id: &str,
    snapshot_content: &str,
) {
    let sql = format!(
        "INSERT INTO lix_internal_state_vtable (\
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

// TODO(parity): Legacy SDK also resolves inheritance on state_by_version.
// Current milestone follows plan.md scope: inheritance is applied on lix_state reads.

simulation_test!(
    lix_state_select_inherits_from_parent_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-child").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-child'",
                &[],
            )
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
                "SELECT entity_id, version_id, inherited_from_version_id, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-inherited'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "entity-inherited");
        assert_text(&rows.rows[0][1], "version-child");
        assert_text(&rows.rows[0][2], "global");
        assert_text(&rows.rows[0][3], "{\"value\":\"global\"}");
    }
);

simulation_test!(
    lix_state_select_prefers_child_row_over_parent,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-child").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-child'",
                &[],
            )
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
                "SELECT version_id, inherited_from_version_id, snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'test_state_schema' \
               AND entity_id = 'entity-override'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "version-child");
        assert_eq!(rows.rows[0][1], Value::Null);
        assert_text(&rows.rows[0][2], "{\"value\":\"child\"}");
    }
);

simulation_test!(
    lix_state_select_child_tombstone_hides_parent_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-child").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-child'",
                &[],
            )
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

        sim.assert_deterministic(rows.rows.clone());
        assert!(rows.rows.is_empty());
    }
);

simulation_test!(
    lix_state_delete_with_inherited_null_filter_deletes_only_local_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.unwrap();

        register_test_schema(&engine).await;
        insert_version(&engine, "version-child").await;
        engine
            .execute(
                "UPDATE lix_active_version SET version_id = 'version-child'",
                &[],
            )
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
                   AND inherited_from_version_id IS NULL",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, inherited_from_version_id, snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'test_state_schema' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.rows.clone());
        assert_eq!(rows.rows.len(), 1);
        assert_text(&rows.rows[0][0], "entity-global");
        assert_text(&rows.rows[0][1], "global");
        assert_text(&rows.rows[0][2], "{\"value\":\"global\"}");
    }
);
