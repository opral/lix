use crate::support;

use lix_engine::{ExecuteOptions, LixError, Value};
use serde_json::json;

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_integer(value: &Value, expected: i64) {
    match value {
        Value::Integer(actual) => assert_eq!(*actual, expected),
        other => panic!("expected integer value '{expected}', got {other:?}"),
    }
}

async fn register_writer_key_test_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .register_schema(&json!({
            "x-lix-key": "wk_writer_key_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "key": { "type": "string" }
            },
            "required": ["key"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

simulation_test!(
    explicit_writer_key_insert_is_accepted_through_state_surface,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = engine.active_version_id().await.unwrap();
        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, writer_key\
                     ) VALUES (\
                     'wk-explicit-insert', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"insert\"}}', '1', 'editor:explicit-insert'\
                     )"
                ),
                &[],
            )
            .await
            .unwrap();

        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-explicit-insert' \
                       AND file_id = 'file-1' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_text(
            &state_row.statements[0].rows[0][0],
            "editor:explicit-insert",
        );

        let workspace_annotation = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_internal_workspace_writer_key \
                     WHERE version_id = '{version_id}' \
                       AND schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-explicit-insert' \
                       AND file_id = 'file-1' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(workspace_annotation.statements[0].rows.len(), 1);
        assert_text(
            &workspace_annotation.statements[0].rows[0][0],
            "editor:explicit-insert",
        );
    }
);

simulation_test!(
    writer_key_only_state_update_is_visible_without_creating_canonical_change,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();
        register_writer_key_test_schema(&engine).await;

        let version_id = engine.active_version_id().await.unwrap();
        engine
            .execute_with_options(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'wk-annotation-only', 'wk_writer_key_schema', 'file-1', '{version_id}', 'lix', '{{\"key\":\"before\"}}', '1'\
                     )"
                ),
                &[],
                ExecuteOptions {
                    writer_key: Some("editor:initial".to_string()),
                },
            )
            .await
            .unwrap();

        let change_count_before = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE schema_key = 'wk_writer_key_schema' \
                   AND entity_id = 'wk-annotation-only'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(change_count_before.statements[0].rows.len(), 1);
        let before_count = match &change_count_before.statements[0].rows[0][0] {
            Value::Integer(value) => *value,
            other => panic!("expected integer change count, got {other:?}"),
        };

        let transaction_version_id = version_id.clone();
        engine
            .transaction(ExecuteOptions::default(), move |tx| {
                Box::pin(async move {
                    tx.execute(
                        &format!(
                            "UPDATE lix_state_by_version \
                             SET writer_key = 'editor:annotation-only' \
                             WHERE schema_key = 'wk_writer_key_schema' \
                               AND entity_id = 'wk-annotation-only' \
                               AND file_id = 'file-1' \
                               AND version_id = '{transaction_version_id}'"
                        ),
                        &[],
                    )
                    .await?;

                    let visible = tx
                        .execute(
                            &format!(
                                "SELECT writer_key \
                                 FROM lix_state_by_version \
                                 WHERE schema_key = 'wk_writer_key_schema' \
                                   AND entity_id = 'wk-annotation-only' \
                                   AND file_id = 'file-1' \
                                   AND version_id = '{transaction_version_id}'"
                            ),
                            &[],
                        )
                        .await?;
                    assert_eq!(visible.statements[0].rows.len(), 1);
                    assert_text(&visible.statements[0].rows[0][0], "editor:annotation-only");
                    Ok::<(), LixError>(())
                })
            })
            .await
            .unwrap();

        let change_count_after = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE schema_key = 'wk_writer_key_schema' \
                   AND entity_id = 'wk-annotation-only'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(change_count_after.statements[0].rows.len(), 1);
        assert_integer(&change_count_after.statements[0].rows[0][0], before_count);

        let state_row = engine
            .execute(
                &format!(
                    "SELECT writer_key \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'wk_writer_key_schema' \
                       AND entity_id = 'wk-annotation-only' \
                       AND file_id = 'file-1' \
                       AND version_id = '{version_id}'"
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(state_row.statements[0].rows.len(), 1);
        assert_text(
            &state_row.statements[0].rows[0][0],
            "editor:annotation-only",
        );
    }
);
