use crate::support;

use lix_engine::Value;
use serde_json::json;
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

async fn register_test_schema(engine: &support::simulation_test::SimulatedLix) {
    let value = serde_json::json!({
        "x-lix-key": "test_state_schema",
        "x-lix-version": "1",
        "type": "object",
        "properties": {
            "value": { "type": "string" }
        },
        "required": ["value"],
        "additionalProperties": false
    });
    engine
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES (lix_json($1), true)",
            &[Value::Text(value.to_string())],
        )
        .await
        .unwrap();
}

async fn ensure_file_descriptor(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
    file_id: &str,
) {
    let existing = engine
        .execute(
            "SELECT entity_id \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = $1 \
               AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await
        .unwrap();
    if !existing.statements[0].rows.is_empty() {
        return;
    }

    let (name, extension) = file_id
        .rsplit_once('.')
        .map(|(name, extension)| (name, Some(extension)))
        .unwrap_or((file_id, None));
    let snapshot = json!({
        "id": file_id,
        "directory_id": null,
        "name": name,
        "extension": extension,
        "metadata": null,
        "hidden": false
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (\
             $1, 'lix_file_descriptor', NULL, $2, NULL, $3, '1'\
             )",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(snapshot),
            ],
        )
        .await
        .unwrap();
}

async fn insert_state_row(
    engine: &support::simulation_test::SimulatedLix,
    entity_id: &str,
    version_id: &str,
    snapshot_content: &str,
) {
    insert_state_row_for_schema(
        engine,
        entity_id,
        "test_state_schema",
        version_id,
        snapshot_content,
    )
    .await;
}

async fn insert_state_row_for_schema(
    engine: &support::simulation_test::SimulatedLix,
    entity_id: &str,
    schema_key: &str,
    version_id: &str,
    snapshot_content: &str,
) {
    ensure_file_descriptor(engine, version_id, "test-file").await;
    let sql = format!(
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
         ) VALUES (\
         '{entity_id}', '{schema_key}', 'test-file', '{version_id}', NULL, '{snapshot_content}', '1'\
         )",
        entity_id = entity_id,
        schema_key = schema_key,
        version_id = version_id,
        snapshot_content = snapshot_content.replace('\'', "''"),
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    lix_state_by_version_insert_rejects_invalid_snapshot_content,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-invalid', 'test_state_schema', 'test-file', 'version-a', NULL, '{\"value\":1}', '1'\
                 )",
                &[],
            )
            .await
            .expect_err("invalid snapshot_content should fail on public insert");

        assert!(
            err.description
                .contains("snapshot_content does not match schema 'test_state_schema' (1)"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_state_by_version_select_exposes_commit_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(&engine, "entity-commit", "version-a", "{\"value\":\"A\"}").await;

        let rows = engine
            .execute(
                "SELECT commit_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-commit' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[2]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        let commit_id = match &rows.statements[0].rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected text commit_id in lix_state_by_version, got {other:?}"),
        };
        assert!(!commit_id.is_empty(), "expected non-empty commit_id");
    }
);

simulation_test!(
    lix_state_by_version_select_scopes_to_version_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(&engine, "entity-sel", "version-a", "{\"value\":\"A\"}").await;
        insert_state_row(&engine, "entity-sel", "version-b", "{\"value\":\"B\"}").await;

        let rows = engine
            .execute(
                "SELECT entity_id, version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-sel' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[1]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-sel");
        assert_text(&rows.statements[0].rows[0][1], "version-a");
        assert_text(&rows.statements[0].rows[0][2], "{\"value\":\"A\"}");
    }
);

simulation_test!(
    lix_state_by_version_select_reads_visible_global_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
        insert_state_row(
            &engine,
            "entity-inherited",
            "global",
            "{\"value\":\"global\"}",
        )
        .await;

        let rows = engine
            .execute(
                "SELECT entity_id, version_id, global, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-inherited' \
                   AND version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[2]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-inherited");
        assert_text(&rows.statements[0].rows[0][1], "version-child");
        assert_boolean_like(&rows.statements[0].rows[0][2], true);
        assert_text(&rows.statements[0].rows[0][3], "{\"value\":\"global\"}");
    }
);

simulation_test!(
    lix_state_by_version_select_prefers_local_row_over_global_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
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
                "SELECT version_id, global, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-override' \
                   AND version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[1]));
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-child");
        assert_boolean_like(&rows.statements[0].rows[0][1], false);
        assert_text(&rows.statements[0].rows[0][2], "{\"value\":\"child\"}");
    }
);

simulation_test!(
    lix_state_by_version_select_unknown_schema_key_returns_schema_not_registered_error,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        let err = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'markdown_v2_document' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect_err("unknown schema key should fail before execution");

        assert_eq!(err.code, "LIX_ERROR_SCHEMA_NOT_REGISTERED");
        assert!(err.description.contains("Schema `markdown_v2_document`"));
        assert!(err
            .description
            .contains("SELECT * FROM lix_registered_schema"));
    }
);

simulation_test!(
    lix_state_by_version_select_local_tombstone_hides_global_row,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-child").await.unwrap();
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
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-tomb' \
                   AND version_id = 'version-child'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert!(rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_state_by_version_select_hides_tracked_tombstones,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-tomb', 'test_state_schema', 'test-file', 'version-a', NULL, '1', '{\"value\":\"live\"}'\
                 )", &[])
            .await
            .unwrap();

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-tomb' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-tomb' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert!(rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_state_by_version_insert_routes_through_state_surface,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-ins', 'test_state_schema', 'test-file', 'version-a', NULL, '1', '{\"value\":\"inserted\"}'\
                 )", &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-ins' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"inserted\"}");
    }
);

simulation_test!(
    lix_state_by_version_insert_supports_placeholders,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)", &[
                    Value::Text("entity-ins-p".to_string()),
                    Value::Text("test_state_schema".to_string()),
                    Value::Text("test-file".to_string()),
                    Value::Text("version-a".to_string()),
                    Value::Null,
                    Value::Text("1".to_string()),
                    Value::Text("{\"value\":\"inserted-p\"}".to_string()),
                ])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-ins-p' \
                   AND file_id = 'test-file'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"inserted-p\"}");
    }
);

simulation_test!(
    lix_state_by_version_insert_on_conflict_do_update_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "file-upsert-bv").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert-bv', 'test_state_schema', 'file-upsert-bv', 'version-a', NULL, '1', '{\"value\":\"A\"}'\
                 )", &[])
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert-bv', 'test_state_schema', 'file-upsert-bv', 'version-a', NULL, '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE \
                 SET snapshot_content = '{\"value\":\"B\"}'", &[])
            .await
            .unwrap();

        let visible = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert-bv' \
                   AND file_id = 'file-upsert-bv' \
                   AND version_id = 'version-a'",
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
                   AND entity_id = 'entity-upsert-bv' \
                   AND file_id = 'file-upsert-bv' \
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
    lix_state_by_version_insert_on_conflict_do_nothing_is_supported,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix_deterministic()
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "file-upsert-bv").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert-bv', 'test_state_schema', 'file-upsert-bv', 'version-a', NULL, '1', '{\"value\":\"A\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-upsert-bv', 'test_state_schema', 'file-upsert-bv', 'version-a', NULL, '1', '{\"value\":\"B\"}'\
                 ) \
                 ON CONFLICT (entity_id, schema_key, file_id, version_id) DO NOTHING", &[])
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upsert-bv' \
                   AND file_id = 'file-upsert-bv' \
                   AND version_id = 'version-a'",
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
    lix_state_by_version_insert_requires_version_id,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-ins-err', 'test_state_schema', 'test-file', NULL, '1', '{\"value\":\"x\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("insert without version_id should fail");

        assert!(
            err.description
                .contains("lix_state_by_version insert requires version_id"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_state_by_version_insert_supports_heterogeneous_row_shapes,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        ensure_file_descriptor(&engine, "version-a", "test-file").await;
        ensure_file_descriptor(&engine, "version-b", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES \
                 ('entity-tracked', 'test_state_schema', 'test-file', 'version-a', NULL, '{\"value\":\"tracked\"}', '1', false), \
                 ('entity-untracked', 'test_state_schema', 'test-file', 'version-b', NULL, '{\"value\":\"untracked\"}', '1', true)",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, version_id, untracked, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(normalize_bool_like_rows(&rows.statements[0].rows, &[2]));
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "entity-tracked");
        assert_text(&rows.statements[0].rows[0][1], "version-a");
        assert_boolean_like(&rows.statements[0].rows[0][2], false);
        assert_text(&rows.statements[0].rows[0][3], "{\"value\":\"tracked\"}");
        assert_text(&rows.statements[0].rows[1][0], "entity-untracked");
        assert_text(&rows.statements[0].rows[1][1], "version-b");
        assert_boolean_like(&rows.statements[0].rows[1][2], true);
        assert_text(&rows.statements[0].rows[1][3], "{\"value\":\"untracked\"}");
    }
);

simulation_test!(
    lix_state_by_version_insert_supports_global_local_and_tracked_rows_together,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        let main_rows = engine
            .execute(
                "SELECT id FROM lix_version WHERE name = 'main' LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        let main_version_id = match &main_rows.statements[0].rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected main version id text, got {other:?}"),
        };
        ensure_file_descriptor(&engine, "global", "test-file").await;
        ensure_file_descriptor(&engine, "version-a", "test-file").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES \
                 ('entity-global-untracked', 'test_state_schema', 'test-file', 'global', NULL, '{\"value\":\"global-untracked\"}', '1', true), \
                 ('entity-local-untracked', 'test_state_schema', 'test-file', 'version-a', NULL, '{\"value\":\"local-untracked\"}', '1', true), \
                 ('entity-global-tracked', 'test_state_schema', 'test-file', 'global', NULL, '{\"value\":\"global-tracked\"}', '1', false)",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, version_id, untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                 ORDER BY entity_id, version_id",
                &[],
            )
            .await
            .unwrap();

        let normalized = normalize_bool_like_rows(&rows.statements[0].rows, &[2])
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if index == 1 {
                            match value {
                                Value::Text(actual) if actual == main_version_id => {
                                    Value::Text("main".to_string())
                                }
                                other => other,
                            }
                        } else {
                            value
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        sim.assert_deterministic(normalized.clone());
        assert_eq!(normalized.len(), 7);
        assert_eq!(
            normalized,
            vec![
                vec![
                    Value::Text("entity-global-tracked".to_string()),
                    Value::Text("main".to_string()),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("entity-global-tracked".to_string()),
                    Value::Text("global".to_string()),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("entity-global-tracked".to_string()),
                    Value::Text("version-a".to_string()),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("entity-global-untracked".to_string()),
                    Value::Text("main".to_string()),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("entity-global-untracked".to_string()),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("entity-global-untracked".to_string()),
                    Value::Text("version-a".to_string()),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("entity-local-untracked".to_string()),
                    Value::Text("version-a".to_string()),
                    Value::Boolean(true),
                ],
            ]
        );
    }
);

simulation_test!(
    lix_state_by_version_update_routes_to_explicit_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(&engine, "entity-upd", "version-a", "{\"value\":\"A\"}").await;
        insert_state_row(&engine, "entity-upd", "version-b", "{\"value\":\"B\"}").await;

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"A-updated\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd' \
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
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"B\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_respects_exact_file_predicate,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-upd-file-scope",
            "version-a",
            "{\"value\":\"before\"}",
        )
        .await;

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-file-scope' \
                   AND file_id = 'other-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT file_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-file-scope' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "test-file");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"before\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_missing_rows_is_noop,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-existing-noop",
            "version-a",
            "{\"value\":\"before\"}",
        )
        .await;
        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-missing-noop' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .expect("missing explicit-version rows should resolve as a no-op");

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND version_id = 'version-a' \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "entity-existing-noop");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"before\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_rejects_identity_column_mutation,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-upd-identity",
            "version-a",
            "{\"value\":\"before\"}",
        )
        .await;

        let err = engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET file_id = 'other-file' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-identity' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .expect_err("identity column mutation should fail");

        assert!(
            err.description
                .contains("does not support changing 'file_id'"),
            "unexpected error: {}",
            err.description
        );

        let rows = engine
            .execute(
                "SELECT file_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-identity' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "test-file");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"before\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_supports_or_selectors,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(&engine, "entity-upd-or-a", "version-a", "{\"value\":\"A\"}").await;
        insert_state_row(&engine, "entity-upd-or-b", "version-a", "{\"value\":\"B\"}").await;

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND version_id = 'version-a' \
                   AND (entity_id = 'entity-upd-or-a' OR entity_id = 'entity-upd-or-b')",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND version_id = 'version-a' \
                   AND entity_id IN ('entity-upd-or-a', 'entity-upd-or-b') \
                 ORDER BY entity_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "entity-upd-or-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"after\"}");
        assert_text(&rows.statements[0].rows[1][0], "entity-upd-or-b");
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"after\"}");
    }
);

// TODO(parity): Legacy SDK supports broader placeholder forms in UPDATE assignments.
// Rust state-surface UPDATE currently requires snapshot_content as a direct literal/parameter expression.

simulation_test!(
    lix_state_by_version_update_supports_multi_version_selectors,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(
            &engine,
            "entity-upd-multi",
            "version-a",
            "{\"value\":\"before-a\"}",
        )
        .await;
        insert_state_row(
            &engine,
            "entity-upd-multi",
            "version-b",
            "{\"value\":\"before-b\"}",
        )
        .await;

        engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-multi' \
                   AND file_id = 'test-file' \
                   AND version_id IN ('version-a', 'version-b')",
                &[],
            )
            .await
            .expect("multi-version update should succeed");

        let rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-multi' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 2);
        assert_text(&rows.statements[0].rows[0][0], "version-a");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"after\"}");
        assert_text(&rows.statements[0].rows[1][0], "version-b");
        assert_text(&rows.statements[0].rows[1][1], "{\"value\":\"after\"}");
    }
);

simulation_test!(
    lix_state_by_version_update_rejects_unknown_assignment_column,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-upd-unknown",
            "version-a",
            "{\"value\":\"before\"}",
        )
        .await;

        let err = engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET bogus = 'x' \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-upd-unknown' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .expect_err("update with unknown assignment should fail");

        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "unexpected error: {}",
            err.description
        );
    }
);

simulation_test!(
    lix_state_by_version_delete_routes_to_explicit_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(&engine, "entity-del", "version-a", "{\"value\":\"A\"}").await;
        insert_state_row(&engine, "entity-del", "version-b", "{\"value\":\"B\"}").await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let materialized = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(materialized.statements[0].rows.clone());
        assert_eq!(materialized.statements[0].rows.len(), 1);
        assert_text(&materialized.statements[0].rows[0][0], "version-b");
        assert_text(&materialized.statements[0].rows[0][1], "{\"value\":\"B\"}");

        let visible = engine
            .execute(
                "SELECT version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(visible.statements[0].rows.clone());
        assert_eq!(visible.statements[0].rows.len(), 1);
        assert_text(&visible.statements[0].rows[0][0], "version-b");
    }
);

simulation_test!(
    lix_state_by_version_delete_respects_exact_file_predicate,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(
            &engine,
            "entity-del-file-scope",
            "version-a",
            "{\"value\":\"before\"}",
        )
        .await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-file-scope' \
                   AND file_id = 'other-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT file_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-file-scope' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert_eq!(rows.statements[0].rows.len(), 1);
        assert_text(&rows.statements[0].rows[0][0], "test-file");
        assert_text(&rows.statements[0].rows[0][1], "{\"value\":\"before\"}");
    }
);

simulation_test!(
    lix_state_by_version_delete_supports_placeholders,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(&engine, "entity-del-p", "version-a", "{\"value\":\"A\"}").await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = $1 \
                   AND file_id = $2 \
                   AND version_id = $3",
                &[
                    Value::Text("entity-del-p".to_string()),
                    Value::Text("test-file".to_string()),
                    Value::Text("version-a".to_string()),
                ],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-p' \
                   AND file_id = 'test-file' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert!(rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_state_by_version_delete_supports_or_selectors,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        insert_state_row(&engine, "entity-del-or-a", "version-a", "{\"value\":\"A\"}").await;
        insert_state_row(&engine, "entity-del-or-b", "version-a", "{\"value\":\"B\"}").await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND version_id = 'version-a' \
                   AND (entity_id = 'entity-del-or-a' OR entity_id = 'entity-del-or-b')",
                &[],
            )
            .await
            .unwrap();

        let rows = engine
            .execute(
                "SELECT entity_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND version_id = 'version-a' \
                   AND entity_id IN ('entity-del-or-a', 'entity-del-or-b')",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(rows.statements[0].rows.clone());
        assert!(rows.statements[0].rows.is_empty());
    }
);

simulation_test!(
    lix_state_by_version_delete_supports_multi_version_selectors,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_test_schema(&engine).await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        insert_state_row(
            &engine,
            "entity-del-multi",
            "version-a",
            "{\"value\":\"before-a\"}",
        )
        .await;
        insert_state_row(
            &engine,
            "entity-del-multi",
            "version-b",
            "{\"value\":\"before-b\"}",
        )
        .await;

        engine
            .execute(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-multi' \
                   AND file_id = 'test-file' \
                   AND version_id IN ('version-a', 'version-b')",
                &[],
            )
            .await
            .expect("multi-version delete should succeed");

        let visible_rows = engine
            .execute(
                "SELECT version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-multi' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(visible_rows.statements[0].rows.clone());
        assert!(visible_rows.statements[0].rows.is_empty());

        let materialized_rows = engine
            .execute(
                "SELECT version_id, snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_state_schema' \
                   AND entity_id = 'entity-del-multi' \
                   AND file_id = 'test-file' \
                 ORDER BY version_id",
                &[],
            )
            .await
            .unwrap();

        sim.assert_deterministic(materialized_rows.statements[0].rows.clone());
        assert!(materialized_rows.statements[0].rows.is_empty());
    }
);
