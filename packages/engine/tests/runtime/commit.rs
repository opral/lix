use crate::support;

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
    change_ids: &BTreeSet<String>,
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
        let Some(commit_snapshot_change_ids) =
            commit_json.get("change_ids").and_then(|v| v.as_array())
        else {
            continue;
        };
        let commit_change_ids = commit_snapshot_change_ids
            .iter()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect::<BTreeSet<_>>();
        if change_ids.is_subset(&commit_change_ids) {
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

async fn change_set_element_change_ids_for_change_set(
    engine: &SimulationEngine,
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

async fn drop_commit_family_live_mirrors(engine: &SimulationEngine) {
    for table in [
        "lix_internal_live_v1_lix_commit",
        "lix_internal_live_v1_lix_change_set_element",
        "lix_internal_live_v1_lix_commit_edge",
        "lix_internal_live_v1_lix_change_author",
    ] {
        engine
            .execute(&format!("DROP TABLE IF EXISTS {table}"), &[])
            .await
            .unwrap();
    }
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

        let change = engine
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
        assert_eq!(change.statements[0].rows.len(), 1);

        let snapshot_id = as_text(&change.statements[0].rows[0][1]);
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
    commit_family_rows_remain_visible_through_state_without_live_mirrors,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;
        engine
            .set_active_account_ids(vec!["acct-runtime".to_string()])
            .await
            .expect("set_active_account_ids should succeed");
        engine.create_named_version("version-main").await.unwrap();

        let previous_commit_id = read_version_ref_commit_id(&engine, "version-main").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'mirrorless-entity', 'test_schema', 'file-1', 'version-main', 'lix', '{\"key\":\"value\"}', '1'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let new_commit_id = read_version_ref_commit_id(&engine, "version-main").await;
        assert_ne!(new_commit_id, previous_commit_id);

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
                 WHERE schema_key = 'test_schema' \
                   AND entity_id = 'mirrorless-entity'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(changes.statements[0].rows.len(), 1);
        let change_id = as_text(&changes.statements[0].rows[0][0]);

        drop_commit_family_live_mirrors(&engine).await;

        let commit_row = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1 \
                   AND version_id = 'version-main' \
                 LIMIT 1",
                &[Value::Text(new_commit_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(commit_row.statements[0].rows.len(), 1);
        let commit_snapshot = parse_json(&commit_row.statements[0].rows[0][0]);
        let change_set_id = commit_snapshot["change_set_id"]
            .as_str()
            .expect("commit state snapshot should include change_set_id")
            .to_string();

        let cse_rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_change_set_element' \
                   AND version_id = 'version-main'",
                &[],
            )
            .await
            .unwrap();
        let mut cse_contains_change = false;
        for row in &cse_rows.statements[0].rows {
            let parsed = parse_json(&row[0]);
            if parsed["change_set_id"] == change_set_id && parsed["change_id"] == change_id {
                cse_contains_change = true;
                break;
            }
        }
        assert!(cse_contains_change);

        let edge_row = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit_edge' \
                   AND entity_id = $1 \
                   AND version_id = 'version-main' \
                 LIMIT 1",
                &[Value::Text(format!("{previous_commit_id}~{new_commit_id}"))],
            )
            .await
            .unwrap();
        assert_eq!(edge_row.statements[0].rows.len(), 1);
        let edge_snapshot = parse_json(&edge_row.statements[0].rows[0][0]);
        assert_eq!(
            edge_snapshot["parent_id"].as_str(),
            Some(previous_commit_id.as_str())
        );
        assert_eq!(
            edge_snapshot["child_id"].as_str(),
            Some(new_commit_id.as_str())
        );

        let author_row = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_change_author' \
                   AND entity_id = $1 \
                   AND version_id = 'version-main' \
                 LIMIT 1",
                &[Value::Text(format!("{change_id}~acct-runtime"))],
            )
            .await
            .unwrap();
        assert_eq!(author_row.statements[0].rows.len(), 1);
        let author_snapshot = parse_json(&author_row.statements[0].rows[0][0]);
        assert_eq!(
            author_snapshot["change_id"].as_str(),
            Some(change_id.as_str())
        );
        assert_eq!(author_snapshot["account_id"].as_str(), Some("acct-runtime"));
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
                 'entity-untracked', 'test_schema', 'file-1', lix_active_version_id(), 'lix', '{\"key\":\"local\"}', '1', true\
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
        engine.create_named_version("version-main").await.unwrap();

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

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

        engine.initialize().await.unwrap();
        register_test_schema(&engine).await;
        engine.create_named_version("version-main").await.unwrap();

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

        let changes = engine
            .execute(
                "SELECT id \
                 FROM lix_internal_change \
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

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
                 FROM lix_internal_change \
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
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");

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
                 FROM lix_internal_change \
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

        let version_id = engine.active_version_id().await.unwrap();
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
