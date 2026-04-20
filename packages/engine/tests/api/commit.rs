use crate::support;

use std::collections::BTreeSet;

use lix_engine::{ExecuteOptions, Value};
use serde_json::Value as JsonValue;
use support::simulation_test::SimulatedLix;

// JS parity scope for this file:
// - packages/sdk/src/state/vtable/commit.test.ts
// - Only non-version/non-inheritance expectations that are implementable now.
//
// Covered now:
// 1) "commit writes business rows to active version; graph edges update globally"
//    - business row writes to change+snapshot are asserted
//    - no edge change rows and exactly one derived commit_edge row are asserted
// 2) "commit with no changes should not create a change set"
//    - adapted to current Rust flow: untracked-only writes create no tracked commit artifacts,
//      but they do create canonical change facts with derived untracked visibility
// 3) "groups changes of a transaction into the same change set for the given version"
//    - both single-statement and explicit BEGIN/COMMIT transaction variants are asserted
//
// Deferred (blocked by versions/inheritance milestones):
// - active/global version movement semantics
// - multi-version commit behavior
// - lineage/ancestry materialization assertions
// - change_author end-to-end via active accounts

async fn register_test_schema(engine: &SimulatedLix) {
    engine
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (\
             lix_json('{\"x-lix-key\":\"test_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"}},\"required\":[\"key\"],\"additionalProperties\":false}')\
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

async fn read_version_ref_commit_id(engine: &SimulatedLix, version_id: &str) -> String {
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
    engine: &SimulatedLix,
    change_ids: &BTreeSet<String>,
) -> Vec<String> {
    let cse_rows = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_change_set_element'",
            &[],
        )
        .await
        .unwrap();

    let mut grouped = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
    for row in &cse_rows.statements[0].rows {
        let cse_json = parse_json(&row[0]);
        let Some(change_set_id) = cse_json.get("change_set_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(change_id) = cse_json.get("change_id").and_then(|v| v.as_str()) else {
            continue;
        };
        grouped
            .entry(change_set_id.to_string())
            .or_default()
            .insert(change_id.to_string());
    }

    grouped
        .into_iter()
        .filter_map(|(change_set_id, grouped_change_ids)| {
            change_ids
                .is_subset(&grouped_change_ids)
                .then_some(change_set_id)
        })
        .collect()
}

async fn change_set_element_change_ids_for_change_set(
    engine: &SimulatedLix,
    change_set_id: &str,
) -> BTreeSet<String> {
    let cse_rows = engine
        .execute(
            "SELECT snapshot_content \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_change_set_element'",
            &[],
        )
        .await
        .unwrap();

    let mut change_ids = BTreeSet::new();
    for row in &cse_rows.statements[0].rows {
        let parsed = parse_json(&row[0]);
        if parsed["change_set_id"] != *change_set_id {
            continue;
        }
        let Some(change_id) = parsed["change_id"].as_str() else {
            continue;
        };
        change_ids.insert(change_id.to_string());
    }

    change_ids
}

simulation_test!(
    commit_writes_business_rows_to_change_and_snapshot_tables,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        // First write establishes the initial global tip in current Rust flow.
        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'para-0', 'test_schema', NULL, 'global', NULL, '{\"key\":\"v0\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let previous_commit_id = read_version_ref_commit_id(&engine, "global").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'para-1', 'test_schema', NULL, 'global', NULL, '{\"key\":\"v1\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let new_commit_id = read_version_ref_commit_id(&engine, "global").await;
        assert_ne!(new_commit_id, previous_commit_id);

        let change = engine
            .execute(
                "SELECT id, snapshot_content \
                 FROM lix_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id = 'para-1' \
                 LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(change.statements[0].rows.len(), 1);

        assert_eq!(
            change.statements[0].rows[0][1],
            Value::Text("{\"key\":\"v1\"}".to_string())
        );

        let edge_entity_id = format!("{previous_commit_id}~{new_commit_id}");

        // JS parity: commit_edge rows are derived; no change rows should be created for lix_commit_edge.
        let edge_change_count = engine
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                 FROM lix_change \
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
    }
);

simulation_test!(
    commit_with_no_tracked_rows_does_not_create_change_set_artifacts,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;

        let before = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change \
                 WHERE schema_key IN ('test_schema', 'lix_commit', 'lix_version_ref', 'lix_change_set_element', 'lix_commit_edge')", &[])
            .await
            .unwrap();
        let before_count = as_i64(&before.statements[0].rows[0][0]);

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES (\
                 'entity-untracked', 'test_schema', NULL, lix_active_version_id(), NULL, '{\"key\":\"local\"}', '1', true\
                 )", &[])
            .await
            .unwrap();

        let after = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change \
                 WHERE schema_key IN ('test_schema', 'lix_commit', 'lix_version_ref', 'lix_change_set_element', 'lix_commit_edge')", &[])
            .await
            .unwrap();
        assert_eq!(as_i64(&after.statements[0].rows[0][0]), before_count + 1);

        let row = engine
            .execute(
                "SELECT snapshot_content, untracked, change_id \
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
        match &row.statements[0].rows[0][2] {
            Value::Text(value) => assert!(!value.is_empty()),
            other => panic!("expected text lixcol_change_id, got {other:?}"),
        }
    }
);

simulation_test!(
    groups_changes_of_single_statement_into_same_change_set,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;
        engine.create_named_version("version-main").await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_schema', NULL, 'version-main', NULL, '{\"key\":\"a\"}', '1'\
                 ), (\
                 'entity-b', 'test_schema', NULL, 'version-main', NULL, '{\"key\":\"b\"}', '1'\
                 )", &[])
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id IN ('entity-a', 'entity-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(changes.statements[0].rows.len(), 2);
        let change_ids: BTreeSet<String> = changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect();

        let matching_change_set_ids = matching_commit_change_set_ids(&engine, &change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
        let change_set_id = &matching_change_set_ids[0];

        let cse_change_ids =
            change_set_element_change_ids_for_change_set(&engine, change_set_id).await;
        assert!(
            change_ids.is_subset(&cse_change_ids),
            "expected change ids {:?} to be subset of change_set {:?}",
            change_ids,
            cse_change_ids
        );
    }
);

simulation_test!(
    groups_changes_across_multiple_statements_in_single_transaction,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;
        engine.create_named_version("version-main").await.unwrap();

        engine
            .execute(
                "BEGIN; \
                 INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-c', 'test_schema', NULL, 'version-main', NULL, '{\"key\":\"c\"}', '1'\
                 ); \
                 INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-d', 'test_schema', NULL, 'version-main', NULL, '{\"key\":\"d\"}', '1'\
                 ); \
                 COMMIT;", &[])
            .await
            .unwrap();

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id IN ('entity-c', 'entity-d')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(changes.statements[0].rows.len(), 2);
        let change_ids: BTreeSet<String> = changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect();

        let matching_change_set_ids = matching_commit_change_set_ids(&engine, &change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
        let change_set_id = &matching_change_set_ids[0];

        let cse_change_ids =
            change_set_element_change_ids_for_change_set(&engine, change_set_id).await;
        assert!(
            change_ids.is_subset(&cse_change_ids),
            "expected change ids {:?} to be subset of change_set {:?}",
            change_ids,
            cse_change_ids
        );
    }
);

simulation_test!(
    explicit_begin_commit_with_multiple_key_value_inserts_creates_single_commit_with_both_changes,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let before_commit_count = engine
            .execute(
                "SELECT COUNT(DISTINCT entity_id) \
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
                "SELECT COUNT(DISTINCT entity_id) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let after_commit_count = as_i64(&after_commit_count.statements[0].rows[0][0]);
        assert_eq!(after_commit_count, before_commit_count + 1);

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id IN ('tx-kv-a', 'tx-kv-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(changes.statements[0].rows.len(), 2);
        let change_ids = changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect::<BTreeSet<_>>();

        let matching_change_set_ids = matching_commit_change_set_ids(&engine, &change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
    }
);

simulation_test!(
    transaction_handle_multiple_key_value_inserts_create_single_commit_with_both_changes,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        let before_commit_count = engine
            .execute(
                "SELECT COUNT(DISTINCT entity_id) \
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
                "SELECT COUNT(DISTINCT entity_id) \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit'",
                &[],
            )
            .await
            .unwrap();
        let after_commit_count = as_i64(&after_commit_count.statements[0].rows[0][0]);
        assert_eq!(after_commit_count, before_commit_count + 1);

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_change \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id IN ('tx-handle-kv-a', 'tx-handle-kv-b')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(changes.statements[0].rows.len(), 2);
        let change_ids = changes.statements[0]
            .rows
            .iter()
            .map(|row| as_text(&row[0]))
            .collect::<BTreeSet<_>>();

        let matching_change_set_ids = matching_commit_change_set_ids(&engine, &change_ids).await;
        assert_eq!(matching_change_set_ids.len(), 1);
    }
);

simulation_test!(
    content_only_update_updates_untracked_version_ref_head,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");

        engine.initialize().await.unwrap();

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('commit-content-only', '/commit-content-only.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .unwrap();

        let version_id = engine.active_version_id().await.unwrap();
        let before = read_version_ref_commit_id(&engine, &version_id).await;
        let before_version_ref_change = engine
            .execute(
                "SELECT change_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_ref' \
                   AND entity_id = $1 \
                   AND untracked = true",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(before_version_ref_change.statements[0].rows.len(), 1);
        let before_version_ref_change_id = match &before_version_ref_change.statements[0].rows[0][0]
        {
            Value::Text(value) => {
                assert!(!value.is_empty());
                value.clone()
            }
            other => panic!("expected text version_ref change_id before update, got {other:?}"),
        };

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

        let version_ref_change = engine
            .execute(
                "SELECT change_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_version_ref' \
                   AND entity_id = $1 \
                   AND untracked = true",
                &[Value::Text(version_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(version_ref_change.statements[0].rows.len(), 1);
        let version_ref_change_id = match &version_ref_change.statements[0].rows[0][0] {
            Value::Text(value) => {
                assert!(!value.is_empty());
                value.clone()
            }
            other => panic!("expected text version_ref change_id, got {other:?}"),
        };

        let commit_snapshot = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_change \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1",
                &[Value::Text(after.clone())],
            )
            .await
            .unwrap();
        assert_eq!(commit_snapshot.statements[0].rows.len(), 1);
        let commit_snapshot = parse_json(&commit_snapshot.statements[0].rows[0][0]);
        let commit_change_ids = commit_snapshot["change_ids"]
            .as_array()
            .expect("commit snapshot should include change_ids")
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<BTreeSet<_>>();
        assert!(
            !commit_change_ids.contains(version_ref_change_id.as_str()),
            "untracked version-ref journal rows must not become commit members: {:?}",
            commit_change_ids
        );

        assert!(
            version_ref_change_id != before_version_ref_change_id,
            "content commit should rotate the visible journaled version_ref row"
        );
    }
);
