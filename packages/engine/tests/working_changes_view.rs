mod support;

use lix_engine::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

static UNIQUE_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_null(value: &Value) {
    assert_eq!(*value, Value::Null, "expected NULL, got {value:?}");
}

fn assert_non_empty_text(value: &Value) {
    match value {
        Value::Text(text) => assert!(!text.is_empty(), "expected non-empty text"),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn assert_not_working_projection_change_id(value: &Value) {
    let text = as_text(value);
    assert!(
        !text.starts_with("working_projection:"),
        "expected non-projection change id, got {text}"
    );
}

fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => *v,
        Value::Text(text) => text
            .parse::<i64>()
            .unwrap_or_else(|error| panic!("expected i64 text value, got '{text}': {error}")),
        other => panic!("expected integer value, got {other:?}"),
    }
}

fn unique_key(prefix: &str) -> String {
    let n = UNIQUE_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n}")
}

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT version_id \
             FROM lix_active_version \
             ORDER BY id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version id query should succeed");
    assert_eq!(result.rows.len(), 1);
    as_text(&result.rows[0][0])
}

async fn rotate_working_commit(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('working-changes-view-seed', 'seed')",
            &[],
        )
        .await
        .expect("seed insert should succeed");
    engine
        .create_checkpoint()
        .await
        .expect("seed checkpoint should succeed");
}

async fn active_version_pointer(engine: &support::simulation_test::SimulationEngine) -> String {
    let result = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version pointer query should succeed");
    assert_eq!(result.rows.len(), 1);
    as_text(&result.rows[0][0])
}

simulation_test!(lix_working_changes_reports_added_rows, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");
    rotate_working_commit(&engine).await;
    let key = unique_key("wc-view-added");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
            &[Value::Text(key.clone())],
        )
        .await
        .expect("insert should succeed");

    let result = engine
        .execute(
            "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1",
            &[Value::Text(key.clone())],
        )
        .await
        .expect("working changes query should succeed");
    let tip_commit_id = active_version_pointer(&engine).await;

    assert_eq!(result.rows.len(), 1);
    assert_eq!(as_text(&result.rows[0][0]), "added");
    assert_null(&result.rows[0][1]);
    assert_non_empty_text(&result.rows[0][2]);
    assert_not_working_projection_change_id(&result.rows[0][2]);
    assert_null(&result.rows[0][3]);
    assert_eq!(as_text(&result.rows[0][4]), tip_commit_id);
});

simulation_test!(
    lix_working_changes_update_reports_modified_rows_against_commit_baseline,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-modified");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        let baseline_commit_id = active_version_pointer(&engine).await;

        engine
            .execute(
                "UPDATE lix_key_value SET value = 'v2' WHERE key = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("update should succeed");

        let result = engine
            .execute(
                "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
                &[Value::Text(key)],
            )
            .await
            .expect("working changes query should succeed");
        let tip_commit_id = active_version_pointer(&engine).await;

        assert_eq!(result.rows.len(), 1);
        assert_eq!(as_text(&result.rows[0][0]), "modified");
        assert_non_empty_text(&result.rows[0][1]);
        assert_non_empty_text(&result.rows[0][2]);
        assert_not_working_projection_change_id(&result.rows[0][2]);
        assert_ne!(as_text(&result.rows[0][1]), as_text(&result.rows[0][2]));
        assert_eq!(as_text(&result.rows[0][3]), baseline_commit_id);
        assert_eq!(as_text(&result.rows[0][4]), tip_commit_id);
    }
);

simulation_test!(
    lix_working_changes_reports_removed_rows_against_commit_baseline,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-removed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                "DELETE FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("delete should succeed");

        let result = engine
        .execute(
            "SELECT status, before_change_id, after_change_id, before_commit_id, after_commit_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
            &[Value::Text(key)],
        )
        .await
        .expect("working changes query should succeed");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(as_text(&result.rows[0][0]), "removed");
        assert_non_empty_text(&result.rows[0][1]);
        assert_null(&result.rows[0][2]);
        assert_non_empty_text(&result.rows[0][3]);
        assert_null(&result.rows[0][4]);
    }
);

simulation_test!(
    lix_working_changes_excludes_unchanged_rows,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-unchanged");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("seed insert should succeed");
        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let result = engine
            .execute(
                "SELECT status, before_change_id, after_change_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value' \
               AND file_id = 'lix' \
               AND entity_id = $1 \
             LIMIT 1",
                &[Value::Text(key)],
            )
            .await
            .expect("working changes query should succeed");

        assert_eq!(result.rows.len(), 0);
    }
);

simulation_test!(
    lix_working_changes_collapses_multiple_tip_entries_for_same_entity,
    simulations = [sqlite, materialization],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-collapse");
        let key_sql = key.replace('\'', "''");
        let active_version_id = active_version_id(&engine).await;
        let active_version_id_sql = active_version_id.replace('\'', "''");

        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     '{key_sql}', 'lix_key_value', 'lix', '{active_version_id_sql}', 'lix', '{{\"key\":\"{key_sql}\",\"value\":\"v2\"}}', '1'\
                     ), (\
                     '{key_sql}', 'lix_key_value', 'lix', '{active_version_id_sql}', 'lix', '{{\"key\":\"{key_sql}\",\"value\":\"v3\"}}', '1'\
                     )"
                ),
                &[],
            )
            .await
            .expect("duplicate internal writes should succeed");

        let tip_change_set = engine
            .execute(
                "SELECT c.change_set_id \
                 FROM lix_active_version av \
                 JOIN lix_version v ON v.id = av.version_id \
                 JOIN lix_commit c ON c.id = v.commit_id \
                 ORDER BY av.id \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("tip change set query should succeed");
        assert_eq!(tip_change_set.rows.len(), 1);
        let tip_change_set_id = as_text(&tip_change_set.rows[0][0]);

        let tip_entry_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id = $2",
                &[Value::Text(tip_change_set_id), Value::Text(key.clone())],
            )
            .await
            .expect("tip entry count query should succeed");
        assert_eq!(
            as_i64(&tip_entry_count.rows[0][0]),
            1,
            "tip change set should collapse duplicate entries for one entity"
        );

        let result = engine
            .execute(
                "SELECT status, before_change_id, after_change_id \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id = $1",
                &[Value::Text(key)],
            )
            .await
            .expect("working changes query should succeed");

        assert_eq!(
            result.rows.len(),
            1,
            "working changes should collapse multiple tip entries for one entity"
        );
        assert_eq!(as_text(&result.rows[0][0]), "added");
        assert_null(&result.rows[0][1]);
        assert_non_empty_text(&result.rows[0][2]);
    }
);

simulation_test!(
    checkpoint_moves_working_changes_into_checkpoint_change_set,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");
        let key = unique_key("wc-view-checkpoint-move");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("working insert should succeed");

        let before_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("pre-checkpoint working changes query should succeed");
        assert_eq!(as_i64(&before_count.rows[0][0]), 1);

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let after_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id = $1",
                &[Value::Text(key.clone())],
            )
            .await
            .expect("post-checkpoint working changes query should succeed");
        assert_eq!(as_i64(&after_count.rows[0][0]), 0);

        let checkpoint_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element \
                 WHERE change_set_id = $1 \
                   AND entity_id = $2 \
                   AND schema_key = 'lix_key_value' \
                   AND file_id = 'lix'",
                &[Value::Text(checkpoint.change_set_id), Value::Text(key)],
            )
            .await
            .expect("checkpoint change_set query should succeed");
        assert_eq!(as_i64(&checkpoint_count.rows[0][0]), 1);
    }
);

simulation_test!(
    lix_working_changes_includes_all_since_checkpoint_commits,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        let key_a = unique_key("wc-view-since-cp-a");
        let key_b = unique_key("wc-view-since-cp-b");

        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key_a.clone())],
            )
            .await
            .expect("first post-checkpoint insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(key_b.clone())],
            )
            .await
            .expect("second post-checkpoint insert should succeed");

        let rows = engine
            .execute(
                "SELECT entity_id, status \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id IN ($1, $2)",
                &[Value::Text(key_a.clone()), Value::Text(key_b.clone())],
            )
            .await
            .expect("working changes query should succeed");

        let mut status_by_entity = BTreeMap::new();
        for row in &rows.rows {
            status_by_entity.insert(as_text(&row[0]), as_text(&row[1]));
        }

        assert_eq!(
            status_by_entity.len(),
            2,
            "working changes should include entities changed in earlier and later commits since checkpoint"
        );
        assert_eq!(status_by_entity.get(&key_a), Some(&"added".to_string()));
        assert_eq!(status_by_entity.get(&key_b), Some(&"added".to_string()));
    }
);

simulation_test!(
    lix_working_changes_preserves_earlier_entity_when_later_commit_is_unrelated,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        let target_key = unique_key("wc-view-target");
        let unrelated_key = unique_key("wc-view-unrelated");

        engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(target_key.clone())],
            )
            .await
            .expect("target insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(unrelated_key)],
            )
            .await
            .expect("unrelated insert should succeed");

        let rows = engine
            .execute(
                "SELECT entity_id, status \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id = $1",
                &[Value::Text(target_key.clone())],
            )
            .await
            .expect("working changes query should succeed");

        assert_eq!(
            rows.rows.len(),
            1,
            "earlier changed entity should remain visible after unrelated later commit"
        );
        assert_eq!(as_text(&rows.rows[0][0]), target_key);
        assert_eq!(as_text(&rows.rows[0][1]), "added");
    }
);

simulation_test!(
    lix_working_changes_isolation_across_versions_uses_per_version_baseline,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        let main_key_a = unique_key("wc-view-main-a");
        let main_key_b = unique_key("wc-view-main-b");
        let branch_key_x = unique_key("wc-view-branch-x");
        let branch_key_y = unique_key("wc-view-branch-y");
        let branch_version = unique_key("wc-view-branch-version");
        let main_version_id = active_version_id(&engine).await;

        engine
            .create_checkpoint()
            .await
            .expect("main checkpoint should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(main_key_a.clone())],
            )
            .await
            .expect("main insert A should succeed");

        engine
            .create_version(lix_engine::CreateVersionOptions {
                id: Some(branch_version.clone()),
                name: Some(branch_version.clone()),
                inherits_from_version_id: None,
                hidden: false,
            })
            .await
            .expect("create version should succeed");

        engine
            .switch_version(branch_version.clone())
            .await
            .expect("switch to branch should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(branch_key_x.clone())],
            )
            .await
            .expect("branch insert X should succeed");
        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(branch_key_y.clone())],
            )
            .await
            .expect("branch insert Y should succeed");

        let branch_rows = engine
            .execute(
                "SELECT entity_id, status \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id IN ($1, $2, $3)",
                &[
                    Value::Text(main_key_a.clone()),
                    Value::Text(branch_key_x.clone()),
                    Value::Text(branch_key_y.clone()),
                ],
            )
            .await
            .expect("branch working changes query should succeed");

        let mut branch_status_by_entity = BTreeMap::new();
        for row in &branch_rows.rows {
            branch_status_by_entity.insert(as_text(&row[0]), as_text(&row[1]));
        }
        assert_eq!(
            branch_status_by_entity.len(),
            2,
            "branch working changes should include only branch changes since its baseline"
        );
        assert_eq!(
            branch_status_by_entity.get(&branch_key_x),
            Some(&"added".to_string())
        );
        assert_eq!(
            branch_status_by_entity.get(&branch_key_y),
            Some(&"added".to_string())
        );
        assert!(
            !branch_status_by_entity.contains_key(&main_key_a),
            "branch should not surface main version working row from earlier baseline"
        );

        engine
            .switch_version(main_version_id)
            .await
            .expect("switch back to main should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, 'v1')",
                &[Value::Text(main_key_b.clone())],
            )
            .await
            .expect("main insert B should succeed");

        let main_rows = engine
            .execute(
                "SELECT entity_id, status \
                 FROM lix_working_changes \
                 WHERE schema_key = 'lix_key_value' \
                   AND file_id = 'lix' \
                   AND entity_id IN ($1, $2, $3, $4)",
                &[
                    Value::Text(main_key_a.clone()),
                    Value::Text(main_key_b.clone()),
                    Value::Text(branch_key_x.clone()),
                    Value::Text(branch_key_y.clone()),
                ],
            )
            .await
            .expect("main working changes query should succeed");

        let mut main_status_by_entity = BTreeMap::new();
        for row in &main_rows.rows {
            main_status_by_entity.insert(as_text(&row[0]), as_text(&row[1]));
        }
        assert_eq!(
            main_status_by_entity.len(),
            2,
            "main working changes should include only main version changes since its baseline"
        );
        assert_eq!(
            main_status_by_entity.get(&main_key_a),
            Some(&"added".to_string())
        );
        assert_eq!(
            main_status_by_entity.get(&main_key_b),
            Some(&"added".to_string())
        );
        assert!(
            !main_status_by_entity.contains_key(&branch_key_x)
                && !main_status_by_entity.contains_key(&branch_key_y),
            "main should not include branch-only working rows"
        );
    }
);
