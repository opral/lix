mod support;

use std::collections::BTreeSet;

use lix_engine::{ExecuteOptions, Value};
use serde_json::Value as JsonValue;
use support::simulation_test::SimulationEngine;

// JS parity scope for this file:
// - packages/sdk/src/state/vtable/commit.test.ts
// - Only non-version/non-inheritance expectations that are implementable now.
//
// Covered now:
// 1) "commit writes business rows to active version; graph edges update globally"
//    - business row writes to change+snapshot are asserted
//    - no edge change rows and exactly one derived commit_edge row are asserted
// 2) "commit with no changes should not create a change set"
//    - adapted to current Rust flow: untracked-only writes create no tracked commit artifacts
// 3) "groups changes of a transaction into the same change set for the given version"
//    - both single-statement and explicit BEGIN/COMMIT transaction variants are asserted
//
// Deferred (blocked by versions/inheritance milestones):
// - active/global version movement semantics
// - multi-version commit behavior
// - lineage/ancestry materialization assertions
// - change_author end-to-end via active accounts

async fn register_test_schema(engine: &SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (\
             '{\"value\":{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}}'\
             )", &[])
        .await
        .unwrap();
}

fn parse_json(value: &Value) -> JsonValue {
    match value {
        Value::Text(text) => serde_json::from_str(text).expect("expected valid json text"),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => *v,
        other => panic!("expected integer value, got {other:?}"),
    }
}

async fn read_version_ref_commit_id(engine: &SimulationEngine, version_id: &str) -> String {
    let result = engine
        .execute(
            &format!(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_ref' \
                   AND entity_id = '{}' \
                 LIMIT 1",
                version_id
            ),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.statements[0].rows.len(), 1);
    let snapshot = parse_json(&result.statements[0].rows[0][0]);
    snapshot["commit_id"]
        .as_str()
        .expect("version tip snapshot should include string commit_id")
        .to_string()
}

async fn matching_commit_change_set_ids(
    engine: &SimulationEngine,
    domain_change_ids: &BTreeSet<String>,
) -> Vec<String> {
    let commit_snapshot_ids = engine
        .execute(
            "SELECT snapshot_id \
             FROM lix_internal_change \
             WHERE schema_key = 'lix_commit'",
            &[],
        )
        .await
        .unwrap();

    let mut matching_change_set_ids = Vec::new();
    for row in &commit_snapshot_ids.statements[0].rows {
        let snapshot_id = as_text(&row[0]);
        let snapshot = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();
        if snapshot.statements[0].rows.is_empty() {
            continue;
        }
        let commit_json = parse_json(&snapshot.statements[0].rows[0][0]);
        let Some(change_ids) = commit_json.get("change_ids").and_then(|v| v.as_array()) else {
            continue;
        };
        let commit_change_ids = change_ids
            .iter()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect::<BTreeSet<_>>();
        if domain_change_ids.is_subset(&commit_change_ids) {
            let change_set_id = commit_json
                .get("change_set_id")
                .and_then(|v| v.as_str())
                .expect("commit snapshot must include change_set_id")
                .to_string();
            matching_change_set_ids.push(change_set_id);
        }
    }

    matching_change_set_ids
}

async fn active_version_id(engine: &SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT version_id \
             FROM lix_active_version \
             ORDER BY id \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    as_text(&result.statements[0].rows[0][0])
}

simulation_test!(
    commit_writes_business_rows_to_change_and_snapshot_tables,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        // First write establishes the initial global tip in current Rust flow.
        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'para-0', 'test_schema', 'file-1', 'global', 'lix', '{\"key\":\"v0\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let previous_commit_id = read_version_ref_commit_id(&engine, "global").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'para-1', 'test_schema', 'file-1', 'global', 'lix', '{\"key\":\"v1\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let new_commit_id = read_version_ref_commit_id(&engine, "global").await;
        assert_ne!(new_commit_id, previous_commit_id);

        let domain_change = engine
            .execute(
                "SELECT id, snapshot_id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id = 'para-1' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(domain_change.statements[0].rows.len(), 1);

        let snapshot_id = as_text(&domain_change.statements[0].rows[0][1]);
        let snapshot = engine
            .execute(
                &format!(
                    "SELECT content FROM lix_internal_snapshot WHERE id = '{}'",
                    snapshot_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(snapshot.statements[0].rows.len(), 1);
        assert_eq!(
            snapshot.statements[0].rows[0][0],
            Value::Text("{\"key\":\"v1\"}".to_string())
        );

        let edge_entity_id = format!("{previous_commit_id}~{new_commit_id}");

        // JS parity: commit_edge rows are derived; no change rows should be created for lix_commit_edge.
        let edge_change_count = engine
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE schema_key = 'lix_commit_edge' \
                   AND entity_id = '{}'",
                    edge_entity_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            edge_change_count.statements[0].rows[0][0],
            Value::Integer(0)
        );

        // The derived edge must still exist in current vtable state.
        let derived_edge = engine
            .execute(
                &format!(
                    "SELECT snapshot_content \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_commit_edge' \
                       AND entity_id = '{}' \
                     LIMIT 1",
                    edge_entity_id
                ),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(derived_edge.statements[0].rows.len(), 1);
        let edge_snapshot = parse_json(&derived_edge.statements[0].rows[0][0]);
        assert_eq!(
            edge_snapshot["parent_id"].as_str(),
            Some(previous_commit_id.as_str())
        );
        assert_eq!(
            edge_snapshot["child_id"].as_str(),
            Some(new_commit_id.as_str())
        );

        // Verify snapshot FK integrity for generated tracked+meta changes.
        let snapshot_fks = engine
            .execute(
                "SELECT snapshot_id \
                 FROM lix_internal_change \
                 WHERE schema_key IN ('test_schema', 'lix_commit', 'lix_version_ref')",
                &[],
            )
            .await
            .unwrap();
        assert!(snapshot_fks.statements[0].rows.len() >= 3);
        for row in &snapshot_fks.statements[0].rows {
            let id = as_text(&row[0]);
            let exists = engine
                .execute(
                    &format!(
                        "SELECT COUNT(*) FROM lix_internal_snapshot WHERE id = '{}'",
                        id
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(exists.statements[0].rows[0][0], Value::Integer(1));
        }
    }
);

simulation_test!(
    commit_with_no_tracked_rows_does_not_create_change_set_artifacts,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        let before = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE schema_key IN ('test_schema', 'lix_commit', 'lix_version_ref', 'lix_change_set_element', 'lix_commit_edge')", &[])
            .await
            .unwrap();
        let before_count = as_i64(&before.statements[0].rows[0][0]);

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES (\
                 'entity-untracked', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"local\"}', '1', true\
                 )", &[])
            .await
            .unwrap();

        let after = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE schema_key IN ('test_schema', 'lix_commit', 'lix_version_ref', 'lix_change_set_element', 'lix_commit_edge')", &[])
            .await
            .unwrap();
        assert_eq!(as_i64(&after.statements[0].rows[0][0]), before_count);

        let row = engine
            .execute(
                "SELECT snapshot_content, untracked \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id = 'entity-untracked'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.statements[0].rows.len(), 1);
        assert_eq!(
            row.statements[0].rows[0][0],
            Value::Text("{\"key\":\"local\"}".to_string())
        );
        match &row.statements[0].rows[0][1] {
            Value::Boolean(value) => assert!(*value),
            Value::Integer(value) => assert_eq!(*value, 1),
            other => panic!("expected true-like untracked marker, got {other:?}"),
        }
    }
);

simulation_test!(
    groups_changes_of_single_statement_into_same_change_set,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"a\"}', '1'\
                 ), (\
                 'entity-b', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"b\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let domain_changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id IN ('entity-a', 'entity-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(domain_changes.statements[0].rows.len(), 2);
        let domain_change_ids: BTreeSet<String> = domain_changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect();

        let matching_change_set_ids =
            matching_commit_change_set_ids(&engine, &domain_change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
        let change_set_id = &matching_change_set_ids[0];

        let cse_rows = engine
            .execute(
                "SELECT cse.snapshot_content \
                 FROM lix_state_by_version cse \
                 JOIN lix_internal_change ch ON ch.id = lix_json_extract(cse.snapshot_content, 'change_id') \
                 WHERE cse.schema_key = 'lix_change_set_element' \
                   AND ch.schema_key = 'test_schema'", &[])
            .await
            .unwrap();

        let cse_for_change_set = cse_rows.statements[0]
            .rows
            .iter()
            .map(|row| parse_json(&row[0]))
            .filter(|snapshot| snapshot["change_set_id"] == *change_set_id)
            .collect::<Vec<_>>();

        let cse_change_ids = cse_for_change_set
            .iter()
            .map(|snapshot| {
                snapshot["change_id"]
                    .as_str()
                    .expect("cse change_id should be string")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert!(
            domain_change_ids.is_subset(&cse_change_ids),
            "expected domain change ids {:?} to be subset of change_set {:?}",
            domain_change_ids,
            cse_change_ids
        );
    }
);

simulation_test!(
    groups_changes_across_multiple_statements_in_single_transaction,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-c', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"c\"}', '1'\
                 ); \
                 INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-d', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"d\"}', '1'\
                 ); \
                 COMMIT;", &[])
            .await
            .unwrap();

        let domain_changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id IN ('entity-c', 'entity-d')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(domain_changes.statements[0].rows.len(), 2);
        let domain_change_ids: BTreeSet<String> = domain_changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect();

        let matching_change_set_ids =
            matching_commit_change_set_ids(&engine, &domain_change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
        let change_set_id = &matching_change_set_ids[0];

        let cse_rows = engine
            .execute(
                "SELECT cse.snapshot_content \
                 FROM lix_state_by_version cse \
                 JOIN lix_internal_change ch ON ch.id = lix_json_extract(cse.snapshot_content, 'change_id') \
                 WHERE cse.schema_key = 'lix_change_set_element' \
                   AND ch.schema_key = 'test_schema'", &[])
            .await
            .unwrap();

        let cse_for_change_set = cse_rows.statements[0]
            .rows
            .iter()
            .map(|row| parse_json(&row[0]))
            .filter(|snapshot| snapshot["change_set_id"] == *change_set_id)
            .collect::<Vec<_>>();

        let cse_change_ids = cse_for_change_set
            .iter()
            .map(|snapshot| {
                snapshot["change_id"]
                    .as_str()
                    .expect("cse change_id should be string")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert!(
            domain_change_ids.is_subset(&cse_change_ids),
            "expected domain change ids {:?} to be subset of change_set {:?}",
            domain_change_ids,
            cse_change_ids
        );
    }
);

simulation_test!(
    explicit_begin_commit_with_multiple_key_value_inserts_creates_single_commit_with_both_changes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let before_commit_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let before_commit_count = as_i64(&before_commit_count.statements[0].rows[0][0]);

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_key_value (key, value) VALUES ('tx-kv-a', 'value-a'); \
                 INSERT INTO lix_key_value (key, value) VALUES ('tx-kv-b', 'value-b'); \
                 COMMIT;",
                &[],
            )
            .await
            .unwrap();

        let values = engine
            .execute(
                "SELECT key, value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-kv-a', 'tx-kv-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(values.statements[0].rows.len(), 2);
        assert_eq!(
            values.statements[0].rows[0],
            vec![
                Value::Text("tx-kv-a".to_string()),
                Value::Text("value-a".to_string())
            ]
        );
        assert_eq!(
            values.statements[0].rows[1],
            vec![
                Value::Text("tx-kv-b".to_string()),
                Value::Text("value-b".to_string())
            ]
        );

        let after_commit_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let after_commit_count = as_i64(&after_commit_count.statements[0].rows[0][0]);
        assert_eq!(after_commit_count, before_commit_count + 1);

        let domain_changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id IN ('tx-kv-a', 'tx-kv-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(domain_changes.statements[0].rows.len(), 2);
        let domain_change_ids = domain_changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect::<BTreeSet<_>>();

        let matching_change_set_ids =
            matching_commit_change_set_ids(&engine, &domain_change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
    }
);

simulation_test!(
    transaction_handle_multiple_key_value_inserts_create_single_commit_with_both_changes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        let before_commit_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let before_commit_count = as_i64(&before_commit_count.statements[0].rows[0][0]);

        engine
            .transaction(ExecuteOptions::default(), |tx| {
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('tx-handle-kv-a', 'value-a')",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('tx-handle-kv-b', 'value-b')",
                        &[],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let values = engine
            .execute(
                "SELECT key, value \
                 FROM lix_key_value \
                 WHERE key IN ('tx-handle-kv-a', 'tx-handle-kv-b') \
                 ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(values.statements[0].rows.len(), 2);
        assert_eq!(
            values.statements[0].rows[0],
            vec![
                Value::Text("tx-handle-kv-a".to_string()),
                Value::Text("value-a".to_string())
            ]
        );
        assert_eq!(
            values.statements[0].rows[1],
            vec![
                Value::Text("tx-handle-kv-b".to_string()),
                Value::Text("value-b".to_string())
            ]
        );

        let after_commit_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let after_commit_count = as_i64(&after_commit_count.statements[0].rows[0][0]);
        assert_eq!(after_commit_count, before_commit_count + 1);

        let domain_changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id IN ('tx-handle-kv-a', 'tx-handle-kv-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(domain_changes.statements[0].rows.len(), 2);
        let domain_change_ids = domain_changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect::<BTreeSet<_>>();

        let matching_change_set_ids =
            matching_commit_change_set_ids(&engine, &domain_change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
    }
);

simulation_test!(
    content_only_update_updates_untracked_version_ref_head,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('commit-content-only', '/commit-content-only.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .unwrap();

        let version_id = active_version_id(&engine).await;
        let before = read_version_ref_commit_id(&engine, &version_id).await;

        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('after') \
                 WHERE id = 'commit-content-only'",
                &[],
            )
            .await
            .unwrap();

        let after = read_version_ref_commit_id(&engine, &version_id).await;
        assert_ne!(after, before);
    }
);
