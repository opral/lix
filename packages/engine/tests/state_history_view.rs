mod support;

use lix_engine::Value;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_non_empty_text(value: &Value) {
    match value {
        Value::Text(actual) => assert!(
            !actual.is_empty(),
            "expected non-empty text value, got empty string"
        ),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn parse_available_columns_from_unknown_column_error(description: &str) -> Vec<String> {
    let marker = "Available columns: ";
    let start = description
        .find(marker)
        .unwrap_or_else(|| panic!("missing available columns marker in error: {description}"));
    let tail = &description[start + marker.len()..];
    let end = tail
        .find('.')
        .unwrap_or_else(|| panic!("missing available columns terminator in error: {description}"));
    let raw = tail[..end].trim();
    if raw == "(unknown)" {
        return Vec::new();
    }
    raw.split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string)
        .collect()
}

async fn register_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_registered_schema',\
             '{\"value\":{\"x-lix-key\":\"test_state_history_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"],\"additionalProperties\":false}}'\
             )", &[])
        .await
        .unwrap();
}

async fn active_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    match &result.statements[0].rows[0][0] {
        Value::Text(text) => text.clone(),
        other => panic!("expected commit_id text, got {other:?}"),
    }
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute("SELECT version_id FROM lix_active_version LIMIT 1", &[])
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    match &result.statements[0].rows[0][0] {
        Value::Text(text) => text.clone(),
        other => panic!("expected version_id text, got {other:?}"),
    }
}

simulation_test!(
    lix_state_history_select_reads_depth_zero_for_active_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'paragraph0', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"initial\"}'\
                 )", &[])
            .await
            .unwrap();

        let commit_id = active_commit_id(&engine).await;
        let version_id = active_version_id(&engine).await;
        let rows = engine
            .execute(
                &format!(
                    "SELECT entity_id, commit_id, root_commit_id, depth, snapshot_content, metadata, commit_created_at, version_id \
                     FROM lix_state_history \
                     WHERE entity_id = 'paragraph0' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{commit_id}'"
                ), &[])
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "paragraph0");
        assert_text(&rows.statements[0].rows[0][1], &commit_id);
        assert_text(&rows.statements[0].rows[0][2], &commit_id);
        assert_eq!(rows.statements[0].rows[0][3], Value::Integer(0));
        assert_text(&rows.statements[0].rows[0][4], "{\"value\":\"initial\"}");
        assert_eq!(rows.statements[0].rows[0][5], Value::Null);
        assert_non_empty_text(&rows.statements[0].rows[0][6]);
        assert_text(&rows.statements[0].rows[0][7], &version_id);
    }
);

simulation_test!(
    lix_state_history_select_reads_multiple_depths,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'paragraph0', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"value0\"}'\
                 )", &[])
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"value1\"}' \
                 WHERE entity_id = 'paragraph0' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"value2\"}' \
                 WHERE entity_id = 'paragraph0' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();

        let root_commit_id = active_commit_id(&engine).await;
        let rows = engine
            .execute(
                &format!(
                    "SELECT depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE entity_id = 'paragraph0' \
                       AND root_commit_id = '{root_commit_id}' \
                     ORDER BY depth ASC"
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 3);
        assert_eq!(rows.statements[0].rows[0][0], Value::Integer(0));
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"value2\"}");
        assert_eq!(rows.statements[0].rows[1][0], Value::Integer(1));
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"value1\"}");
        assert_eq!(rows.statements[0].rows[2][0], Value::Integer(2));
        assert_text(&rows.statements[0].rows[2][1], "{\"value\":\"value0\"}");
    }
);

simulation_test!(
    lix_state_history_exposes_commit_created_at,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'history-created-at', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"value0\"}'\
                 )", &[])
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"value1\"}' \
                 WHERE entity_id = 'history-created-at' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();

        let root_commit_id = active_commit_id(&engine).await;
        let rows = engine
            .execute(
                &format!(
                    "SELECT depth, commit_id, commit_created_at \
                     FROM lix_state_history \
                     WHERE entity_id = 'history-created-at' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{root_commit_id}' \
                     ORDER BY depth ASC"
                ),
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_eq!(rows.statements[0].rows[0][0], Value::Integer(0));
        assert_non_empty_text(&rows.statements[0].rows[0][1]);
        assert_non_empty_text(&rows.statements[0].rows[0][2]);
        assert_eq!(rows.statements[0].rows[1][0], Value::Integer(1));
        assert_non_empty_text(&rows.statements[0].rows[1][1]);
        assert_non_empty_text(&rows.statements[0].rows[1][2]);
    }
);

simulation_test!(
    lix_state_history_unknown_column_diagnostic_matches_select_star_columns,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'history-diagnostic-state', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"value0\"}'\
                 )", &[])
            .await
            .unwrap();

        let root_commit_id = active_commit_id(&engine).await;
        let select_star = engine
            .execute(
                &format!(
                    "SELECT * \
                     FROM lix_state_history \
                     WHERE entity_id = 'history-diagnostic-state' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{root_commit_id}' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .expect("SELECT * on lix_state_history should succeed");
        assert_eq!(select_star.statements[0].rows.len(), 1);
        assert!(
            select_star.statements[0]
                .columns
                .contains(&"commit_created_at".to_string()),
            "state history select-star columns should expose commit_created_at"
        );

        let error = engine
            .execute(
                &format!(
                    "SELECT bogus \
                     FROM lix_state_history \
                     WHERE entity_id = 'history-diagnostic-state' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{root_commit_id}' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .expect_err("unknown state-history column read should fail");
        assert_eq!(error.code, "LIX_ERROR_SQL_UNKNOWN_COLUMN");

        let available_columns =
            parse_available_columns_from_unknown_column_error(&error.description);
        assert_eq!(
            available_columns, select_star.statements[0].columns,
            "unknown-column diagnostics should list the same columns as `SELECT *` on lix_state_history. error: {}",
            error.description
        );
    }
);

simulation_test!(
    lix_state_history_select_reads_specific_root_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity1', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"initial\"}'\
                 )", &[])
            .await
            .unwrap();
        let insert_commit_id = active_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"updated\"}' \
                 WHERE entity_id = 'entity1' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();
        let update_commit_id = active_commit_id(&engine).await;

        let at_insert = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, root_commit_id, depth \
                     FROM lix_state_history \
                     WHERE entity_id = 'entity1' \
                       AND root_commit_id = '{insert_commit_id}' \
                     ORDER BY depth ASC"
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(at_insert.statements[0].rows.clone());
        assert_eq!(at_insert.statements[0].rows.len(), 1);
        assert_text(
            &at_insert.statements[0].rows[0][0],
            "{\"value\":\"initial\"}",
        );
        assert_text(&at_insert.statements[0].rows[0][1], &insert_commit_id);
        assert_eq!(at_insert.statements[0].rows[0][2], Value::Integer(0));

        let at_update = engine
            .execute(
                &format!(
                    "SELECT snapshot_content, root_commit_id, depth \
                     FROM lix_state_history \
                     WHERE entity_id = 'entity1' \
                       AND root_commit_id = '{update_commit_id}' \
                     ORDER BY depth ASC"
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(at_update.statements[0].rows.clone());
        assert_eq!(at_update.statements[0].rows.len(), 2);
        assert_text(
            &at_update.statements[0].rows[0][0],
            "{\"value\":\"updated\"}",
        );
        assert_text(&at_update.statements[0].rows[0][1], &update_commit_id);
        assert_eq!(at_update.statements[0].rows[0][2], Value::Integer(0));
        assert_text(
            &at_update.statements[0].rows[1][0],
            "{\"value\":\"initial\"}",
        );
        assert_text(&at_update.statements[0].rows[1][1], &update_commit_id);
        assert_eq!(at_update.statements[0].rows[1][2], Value::Integer(1));
    }
);

simulation_test!(
    lix_state_history_rejects_insert_update_delete,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();

        let insert_err = engine
            .execute(
                "INSERT INTO lix_state_history (entity_id) VALUES ('x')",
                &[],
            )
            .await
            .expect_err("INSERT on lix_state_history should fail");
        assert_eq!(insert_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

        let update_err = engine
            .execute("UPDATE lix_state_history SET entity_id = 'x'", &[])
            .await
            .expect_err("UPDATE on lix_state_history should fail");
        assert_eq!(update_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");

        let delete_err = engine
            .execute("DELETE FROM lix_state_history", &[])
            .await
            .expect_err("DELETE on lix_state_history should fail");
        assert_eq!(delete_err.code, "LIX_ERROR_READ_ONLY_VIEW_WRITE_DENIED");
    }
);

simulation_test!(
    lix_state_history_stays_sparse_when_only_other_entities_change,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-a', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"a0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        let entity_a_commit_id = active_commit_id(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-b', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"b0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"b1\"}' \
                 WHERE entity_id = 'entity-b' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();
        let latest_root_commit_id = active_commit_id(&engine).await;

        let rows = engine
            .execute(
                &format!(
                    "SELECT commit_id, root_commit_id, depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE entity_id = 'entity-a' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{latest_root_commit_id}' \
                     ORDER BY depth ASC"
                ),
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], &entity_a_commit_id);
        assert_text(&rows.statements[0].rows[0][1], &latest_root_commit_id);
        assert_eq!(rows.statements[0].rows[0][2], Value::Integer(2));
        assert_text(&rows.statements[0].rows[0][3], "{\"value\":\"a0\"}");
    }
);

simulation_test!(
    lix_state_history_depth_zero_exists_only_for_entities_changed_at_root,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-a', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"a0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-b', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"b0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"b1\"}' \
                 WHERE entity_id = 'entity-b' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();
        let latest_root_commit_id = active_commit_id(&engine).await;

        let entity_a_depth_zero = engine
            .execute(
                &format!(
                    "SELECT depth \
                     FROM lix_state_history \
                     WHERE entity_id = 'entity-a' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{latest_root_commit_id}' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(entity_a_depth_zero.statements[0].rows.len(), 0);

        let entity_b_depth_zero = engine
            .execute(
                &format!(
                    "SELECT commit_id, root_commit_id, depth, snapshot_content \
                     FROM lix_state_history \
                     WHERE entity_id = 'entity-b' \
                       AND schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{latest_root_commit_id}' \
                       AND depth = 0"
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(entity_b_depth_zero.statements[0].rows.clone());
        assert_eq!(entity_b_depth_zero.statements[0].rows.len(), 1);
        assert_text(
            &entity_b_depth_zero.statements[0].rows[0][0],
            &latest_root_commit_id,
        );
        assert_text(
            &entity_b_depth_zero.statements[0].rows[0][1],
            &latest_root_commit_id,
        );
        assert_eq!(
            entity_b_depth_zero.statements[0].rows[0][2],
            Value::Integer(0)
        );
        assert_text(
            &entity_b_depth_zero.statements[0].rows[0][3],
            "{\"value\":\"b1\"}",
        );
    }
);

simulation_test!(
    lix_state_history_has_no_duplicate_depth_rows_per_entity_and_root_commit,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-a', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"a0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-b', 'test_state_history_schema', 'f0', 'lix', '1', '{\"value\":\"b0\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();
        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"b1\"}' \
                 WHERE entity_id = 'entity-b' \
                   AND schema_key = 'test_state_history_schema' \
                   AND file_id = 'f0'",
                &[],
            )
            .await
            .unwrap();
        let latest_root_commit_id = active_commit_id(&engine).await;

        let duplicates = engine
            .execute(
                &format!(
                    "SELECT entity_id, root_commit_id, depth, COUNT(*) AS count \
                     FROM lix_state_history \
                     WHERE schema_key = 'test_state_history_schema' \
                       AND root_commit_id = '{latest_root_commit_id}' \
                     GROUP BY entity_id, root_commit_id, depth \
                     HAVING COUNT(*) > 1"
                ),
                &[],
            )
            .await
            .unwrap();
        sim.assert_deterministic(duplicates.statements[0].rows.clone());
        assert_eq!(duplicates.statements[0].rows.len(), 0);
    }
);

// TODO(m27-parity): Port checkpoint label and ancestor/descendant range filters from
// packages/sdk/src/state-history/schema.test.ts once commit labels/query-filter helpers land.
