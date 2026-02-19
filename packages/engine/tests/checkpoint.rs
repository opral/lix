mod support;

use lix_engine::Value;

fn as_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("expected text value, got {other:?}"),
    }
}

fn as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(value) => *value,
        other => panic!("expected integer value, got {other:?}"),
    }
}

async fn active_version_pointer(
    engine: &support::simulation_test::SimulationEngine,
) -> (String, String, String) {
    let result = engine
        .execute(
            "SELECT av.version_id, v.commit_id, v.working_commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(result.rows.len(), 1);
    (
        as_text(&result.rows[0][0]),
        as_text(&result.rows[0][1]),
        as_text(&result.rows[0][2]),
    )
}

simulation_test!(
    checkpoint_noop_returns_tip_and_keeps_version_pointer,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");

        engine.init().await.expect("init should succeed");

        let (_version_id, before_commit_id, before_working_commit_id) =
            active_version_pointer(&engine).await;
        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed");
        let (_version_id, after_commit_id, after_working_commit_id) =
            active_version_pointer(&engine).await;

        assert_eq!(checkpoint.id, before_commit_id);
        assert_eq!(after_commit_id, before_commit_id);
        assert_eq!(after_working_commit_id, before_working_commit_id);
    }
);

simulation_test!(
    checkpoint_promotes_working_and_creates_new_working,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-test-key', 'before')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let (version_id, before_tip_commit_id, before_working_commit_id) =
            active_version_pointer(&engine).await;
        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("create_checkpoint should succeed");
        let (_version_id, after_tip_commit_id, after_working_commit_id) =
            active_version_pointer(&engine).await;

        assert_eq!(checkpoint.id, before_working_commit_id);
        assert_eq!(after_tip_commit_id, checkpoint.id);
        assert_ne!(after_working_commit_id, checkpoint.id);

        let previous_tip_to_checkpoint = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_commit_edge \
             WHERE parent_id = $1 \
               AND child_id = $2",
                &[
                    Value::Text(before_tip_commit_id.clone()),
                    Value::Text(checkpoint.id.clone()),
                ],
            )
            .await
            .expect("previous tip edge query should succeed");
        assert_eq!(as_i64(&previous_tip_to_checkpoint.rows[0][0]), 1);

        let checkpoint_to_new_working = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_commit_edge \
             WHERE parent_id = $1 \
               AND child_id = $2",
                &[
                    Value::Text(checkpoint.id.clone()),
                    Value::Text(after_working_commit_id.clone()),
                ],
            )
            .await
            .expect("checkpoint to new working edge query should succeed");
        assert_eq!(as_i64(&checkpoint_to_new_working.rows[0][0]), 1);

        let checkpoint_label = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_entity_label el \
             JOIN lix_label l ON l.id = el.label_id \
             WHERE el.entity_id = $1 \
               AND el.schema_key = 'lix_commit' \
               AND el.file_id = 'lix' \
               AND l.name = 'checkpoint'",
                &[Value::Text(checkpoint.id.clone())],
            )
            .await
            .expect("checkpoint label query should succeed");
        assert_eq!(as_i64(&checkpoint_label.rows[0][0]), 1);

        let working_change_set = engine
            .execute(
                "SELECT change_set_id \
             FROM lix_commit \
             WHERE id = $1 \
             LIMIT 1",
                &[Value::Text(after_working_commit_id)],
            )
            .await
            .expect("new working change_set query should succeed");
        assert_eq!(working_change_set.rows.len(), 1);
        let new_working_change_set_id = as_text(&working_change_set.rows[0][0]);

        let checkpoint_contains_pre_checkpoint_entity = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_change_set_element \
             WHERE change_set_id = $1 \
               AND entity_id = 'checkpoint-test-key' \
               AND schema_key = 'lix_key_value' \
               AND file_id = 'lix'",
                &[Value::Text(checkpoint.change_set_id.clone())],
            )
            .await
            .expect("checkpoint change_set should contain pre-checkpoint entity change");
        assert_eq!(
            as_i64(&checkpoint_contains_pre_checkpoint_entity.rows[0][0]),
            1
        );

        let new_working_elements = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_change_set_element \
             WHERE change_set_id = $1",
                &[Value::Text(new_working_change_set_id.clone())],
            )
            .await
            .expect("new working elements query should succeed");
        assert_eq!(as_i64(&new_working_elements.rows[0][0]), 0);

        let new_working_contains_pre_checkpoint_entity = engine
            .execute(
                "SELECT COUNT(*) \
             FROM lix_change_set_element \
             WHERE change_set_id = $1 \
               AND entity_id = 'checkpoint-test-key' \
               AND schema_key = 'lix_key_value' \
               AND file_id = 'lix'",
                &[Value::Text(new_working_change_set_id)],
            )
            .await
            .expect("new working change_set should not contain pre-checkpoint entity change");
        assert_eq!(
            as_i64(&new_working_contains_pre_checkpoint_entity.rows[0][0]),
            0
        );

        let version_row = engine
            .execute(
                "SELECT COUNT(*) FROM lix_version WHERE id = $1",
                &[Value::Text(version_id)],
            )
            .await
            .expect("version existence query should succeed");
        assert_eq!(as_i64(&version_row.rows[0][0]), 1);
    }
);

simulation_test!(checkpoint_includes_tombstone_changes, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-tombstone-key', 'before')",
            &[],
        )
        .await
        .expect("seed key insert should succeed");

    engine
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should succeed");

    engine
        .execute(
            "DELETE FROM lix_key_value WHERE key = 'checkpoint-tombstone-key'",
            &[],
        )
        .await
        .expect("delete should succeed");

    let checkpoint = engine
        .create_checkpoint()
        .await
        .expect("checkpoint with tombstone should succeed");

    let tombstone_in_checkpoint = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_change_set_element cse \
             JOIN lix_change c ON c.id = cse.change_id \
             WHERE cse.change_set_id = $1 \
               AND cse.entity_id = 'checkpoint-tombstone-key' \
               AND cse.schema_key = 'lix_key_value' \
               AND cse.file_id = 'lix' \
               AND c.snapshot_content IS NULL",
            &[Value::Text(checkpoint.change_set_id)],
        )
        .await
        .expect("checkpoint change_set tombstone query should succeed");
    assert_eq!(as_i64(&tombstone_in_checkpoint.rows[0][0]), 1);
});

simulation_test!(
    checkpoint_includes_file_descriptor_tombstone_changes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('checkpoint-file-tombstone', '/checkpoint-file-tombstone.json', '{\"before\":true}')",
                &[],
            )
            .await
            .expect("seed file insert should succeed");

        engine
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");

        engine
            .execute(
                "DELETE FROM lix_file WHERE id = 'checkpoint-file-tombstone'",
                &[],
            )
            .await
            .expect("file delete should succeed");

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint with file tombstone should succeed");

        let descriptor_tombstone_in_checkpoint = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_change_set_element cse \
                 JOIN lix_change c ON c.id = cse.change_id \
                 WHERE cse.change_set_id = $1 \
                   AND cse.entity_id = 'checkpoint-file-tombstone' \
                   AND cse.schema_key = 'lix_file_descriptor' \
                   AND cse.file_id = 'lix' \
                   AND c.snapshot_content IS NULL",
                &[Value::Text(checkpoint.change_set_id)],
            )
            .await
            .expect("checkpoint change_set file tombstone query should succeed");
        assert_eq!(as_i64(&descriptor_tombstone_in_checkpoint.rows[0][0]), 1);
    }
);

fn parse_parent_commit_ids(value: &Value) -> Vec<String> {
    match value {
        Value::Null => Vec::new(),
        Value::Text(raw) => {
            let mut ids: Vec<String> =
                serde_json::from_str(raw).expect("parent_commit_ids should be valid JSON array");
            ids.sort();
            ids
        }
        other => panic!("expected text parent_commit_ids, got {other:?}"),
    }
}

simulation_test!(
    checkpoint_merges_existing_working_parents_with_previous_tip,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-parent-merge-seed', 'v1')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        engine
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should succeed");

        let (_version_id, _tip_before_second_write, working_commit_id) =
            active_version_pointer(&engine).await;

        let working_parent_row = engine
            .execute(
                "SELECT parent_commit_ids FROM lix_commit WHERE id = $1 LIMIT 1",
                &[Value::Text(working_commit_id.clone())],
            )
            .await
            .expect("working parent lookup should succeed");
        assert_eq!(working_parent_row.rows.len(), 1);
        let old_working_parents = parse_parent_commit_ids(&working_parent_row.rows[0][0]);
        assert!(
            !old_working_parents.is_empty(),
            "working commit should already have at least one parent"
        );
        for parent_id in &old_working_parents {
            let parent_exists = engine
                .execute(
                    "SELECT COUNT(*) FROM lix_commit WHERE id = $1",
                    &[Value::Text(parent_id.clone())],
                )
                .await
                .expect("parent commit existence query should succeed");
            assert_eq!(as_i64(&parent_exists.rows[0][0]), 1);
        }

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-parent-merge-next', 'v2')",
                &[],
            )
            .await
            .expect("working insert should succeed");

        let (_version_id, previous_tip_id, _current_working_commit_id) =
            active_version_pointer(&engine).await;

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        assert_eq!(checkpoint.id, working_commit_id);

        let parent_row = engine
            .execute(
                "SELECT parent_commit_ids \
                 FROM lix_commit \
                 WHERE id = $1 \
                 LIMIT 1",
                &[Value::Text(checkpoint.id.clone())],
            )
            .await
            .expect("checkpoint parent query should succeed");
        assert_eq!(parent_row.rows.len(), 1);

        let actual_parents = parse_parent_commit_ids(&parent_row.rows[0][0]);
        let mut expected_parents = old_working_parents;
        if !expected_parents.iter().any(|id| id == &previous_tip_id) {
            expected_parents.push(previous_tip_id);
        }
        expected_parents.sort();
        expected_parents.dedup();
        assert_eq!(actual_parents, expected_parents);

        for parent_id in &expected_parents {
            let edge_count = engine
                .execute(
                    "SELECT COUNT(*) \
                     FROM lix_commit_edge \
                     WHERE parent_id = $1 \
                       AND child_id = $2",
                    &[
                        Value::Text(parent_id.clone()),
                        Value::Text(checkpoint.id.clone()),
                    ],
                )
                .await
                .expect("checkpoint parent edge query should succeed");
            assert_eq!(as_i64(&edge_count.rows[0][0]), 1);
        }
    }
);

simulation_test!(
    checkpoint_history_from_new_working_root_includes_prior_data,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.init().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-history-key', 'before')",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let checkpoint = engine
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        let (_version_id, tip_commit_id, new_working_commit_id) =
            active_version_pointer(&engine).await;
        assert_eq!(tip_commit_id, checkpoint.id);

        let history = engine
            .execute(
                &format!(
                    "SELECT key, value, lixcol_root_commit_id, lixcol_depth \
                     FROM lix_key_value_history \
                     WHERE key = 'checkpoint-history-key' \
                       AND lixcol_root_commit_id = '{}' \
                     ORDER BY lixcol_depth ASC",
                    new_working_commit_id
                ),
                &[],
            )
            .await
            .expect("history query should succeed");

        assert_eq!(history.rows.len(), 1);
        assert_eq!(as_text(&history.rows[0][0]), "checkpoint-history-key");
        assert_eq!(as_text(&history.rows[0][1]), "before");
        assert_eq!(as_text(&history.rows[0][2]), new_working_commit_id);
        assert!(as_i64(&history.rows[0][3]) >= 1);
    }
);

simulation_test!(checkpoint_topology_remains_acyclic, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-acyclic-key', 'v1')",
            &[],
        )
        .await
        .expect("seed insert should succeed");
    engine
        .create_checkpoint()
        .await
        .expect("first checkpoint should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-acyclic-key-2', 'v2')",
            &[],
        )
        .await
        .expect("second insert should succeed");
    engine
        .create_checkpoint()
        .await
        .expect("second checkpoint should succeed");

    let cycle_count = engine
        .execute(
            "WITH RECURSIVE walk(start_id, id, depth) AS ( \
               SELECT c.id, c.id, 0 \
               FROM lix_commit c \
               UNION ALL \
               SELECT w.start_id, ce.parent_id, w.depth + 1 \
               FROM walk w \
               JOIN lix_commit_edge ce ON ce.child_id = w.id \
               WHERE w.depth < 128 \
             ), \
             cycles AS ( \
               SELECT DISTINCT start_id \
               FROM walk \
               WHERE depth > 0 \
                 AND id = start_id \
             ) \
             SELECT COUNT(*) FROM cycles",
            &[],
        )
        .await
        .expect("cycle detection query should succeed");
    assert_eq!(as_i64(&cycle_count.rows[0][0]), 0);
});

simulation_test!(checkpoint_has_no_orphaned_commits, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.init().await.expect("init should succeed");

    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-orphan-key', 'v1')",
            &[],
        )
        .await
        .expect("seed insert should succeed");

    engine
        .create_checkpoint()
        .await
        .expect("checkpoint should succeed");

    let commits = engine
        .execute("SELECT id FROM lix_commit", &[])
        .await
        .expect("commit list query should succeed");

    for row in &commits.rows {
        let commit_id = as_text(&row[0]);

        let has_parent_edge = engine
            .execute(
                "SELECT COUNT(*) FROM lix_commit_edge WHERE child_id = $1",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .expect("parent edge query should succeed");
        let has_child_edge = engine
            .execute(
                "SELECT COUNT(*) FROM lix_commit_edge WHERE parent_id = $1",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .expect("child edge query should succeed");
        let referenced_as_tip = engine
            .execute(
                "SELECT COUNT(*) FROM lix_version WHERE commit_id = $1",
                &[Value::Text(commit_id.clone())],
            )
            .await
            .expect("version tip reference query should succeed");
        let referenced_as_working = engine
            .execute(
                "SELECT COUNT(*) FROM lix_version WHERE working_commit_id = $1",
                &[Value::Text(commit_id)],
            )
            .await
            .expect("version working reference query should succeed");

        let has_connections = as_i64(&has_parent_edge.rows[0][0]) > 0
            || as_i64(&has_child_edge.rows[0][0]) > 0
            || as_i64(&referenced_as_tip.rows[0][0]) > 0
            || as_i64(&referenced_as_working.rows[0][0]) > 0;
        assert!(has_connections);
    }
});
