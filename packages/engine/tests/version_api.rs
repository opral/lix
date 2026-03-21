mod support;

use lix_engine::{CreateVersionOptions, MergeOutcome, MergeVersionOptions, Value};

fn value_as_text(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        Value::Integer(value) => value.to_string(),
        other => panic!("expected text-like value, got {other:?}"),
    }
}

fn value_as_bool(value: &Value) -> bool {
    match value {
        Value::Boolean(value) => *value,
        Value::Integer(value) => *value != 0,
        Value::Text(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
        other => panic!("expected boolean-compatible value, got {other:?}"),
    }
}

async fn active_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let active = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version commit query should succeed");
    assert_eq!(active.statements[0].rows.len(), 1);
    value_as_text(&active.statements[0].rows[0][0])
}

async fn version_commit_id(
    engine: &support::simulation_test::SimulationEngine,
    version_id: &str,
) -> String {
    let result = engine
        .execute(
            "SELECT commit_id FROM lix_version WHERE id = $1 LIMIT 1",
            &[Value::Text(version_id.to_string())],
        )
        .await
        .expect("version commit query should succeed");
    assert_eq!(result.statements[0].rows.len(), 1);
    value_as_text(&result.statements[0].rows[0][0])
}

async fn key_value_value(
    engine: &support::simulation_test::SimulationEngine,
    key: &str,
) -> Option<String> {
    let result = engine
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1 LIMIT 1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("key value query should succeed");
    result.statements[0]
        .rows
        .first()
        .and_then(|row| row.first())
        .map(value_as_text)
}

async fn merge_commit_parent_ids(
    engine: &support::simulation_test::SimulationEngine,
    commit_id: &str,
) -> Vec<String> {
    let result = engine
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1 ORDER BY parent_id",
            &[Value::Text(commit_id.to_string())],
        )
        .await
        .expect("merge commit parents query should succeed");
    result.statements[0]
        .rows
        .iter()
        .map(|row| value_as_text(&row[0]))
        .collect()
}

simulation_test!(create_version_defaults_to_active_parent, |sim| async move {
    let engine = sim
        .boot_simulated_engine_deterministic()
        .await
        .expect("boot_simulated_engine_deterministic should succeed");
    engine.initialize().await.expect("init should succeed");

    let active_before = engine
        .execute(
            "SELECT av.version_id, v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    let active_version_id = value_as_text(&active_before.statements[0].rows[0][0]);
    let active_commit_id = value_as_text(&active_before.statements[0].rows[0][1]);

    let created = engine
        .create_version(CreateVersionOptions::default())
        .await
        .expect("create_version should succeed");

    assert!(!created.id.is_empty());
    assert_eq!(created.name, created.id);

    let created_row = engine
        .execute(
            "SELECT id, name, hidden, commit_id \
             FROM lix_version \
             WHERE id = $1",
            &[Value::Text(created.id.clone())],
        )
        .await
        .expect("created version query should succeed");
    assert_eq!(created_row.statements[0].rows.len(), 1);
    let row = &created_row.statements[0].rows[0];
    assert_eq!(value_as_text(&row[0]), created.id);
    assert_eq!(value_as_text(&row[1]), created.name);
    assert!(!value_as_bool(&row[2]));
    assert_eq!(value_as_text(&row[3]), active_commit_id);
    let baseline_row = engine
        .execute(
            "SELECT checkpoint_commit_id \
             FROM lix_internal_last_checkpoint \
             WHERE version_id = $1",
            &[Value::Text(created.id.clone())],
        )
        .await
        .expect("baseline pointer query should succeed");
    assert_eq!(baseline_row.statements[0].rows.len(), 1);
    assert_eq!(
        value_as_text(&baseline_row.statements[0].rows[0][0]),
        active_commit_id
    );

    let active_after = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .expect("active version query after create should succeed");
    assert_eq!(
        value_as_text(&active_after.statements[0].rows[0][0]),
        active_version_id
    );
});

simulation_test!(
    tracked_write_moves_active_commit_id_off_global,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        let active_before = engine
            .execute(
                "SELECT av.version_id, v.commit_id \
                 FROM lix_active_version av \
                 JOIN lix_version v ON v.id = av.version_id \
                 ORDER BY av.id \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(active_before.statements[0].rows.len(), 1);
        let active_version_id = value_as_text(&active_before.statements[0].rows[0][0]);
        assert_ne!(active_version_id, "global");

        engine
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('working-pointer-regression', '1')",
                &[],
            )
            .await
            .expect("tracked write should succeed");

        let active_after = engine
            .execute(
                "SELECT av.version_id, v.commit_id \
                 FROM lix_active_version av \
                 JOIN lix_version v ON v.id = av.version_id \
                 ORDER BY av.id \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(active_after.statements[0].rows.len(), 1);
        assert_eq!(
            value_as_text(&active_after.statements[0].rows[0][0]),
            active_version_id
        );
        assert_ne!(
            value_as_text(&active_after.statements[0].rows[0][1]),
            "global"
        );
    }
);

simulation_test!(
    content_only_update_moves_active_commit_pointer,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine_deterministic()
            .await
            .expect("boot_simulated_engine_deterministic should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('version-api-content-only', '/version-api-content-only.md', lix_text_encode('before'))",
                &[],
            )
            .await
            .expect("seed insert should succeed");

        let before_commit = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('after') \
                 WHERE id = 'version-api-content-only'",
                &[],
            )
            .await
            .expect("content-only update should succeed");

        let after_commit = active_version_commit_id(&engine).await;
        assert_ne!(after_commit, before_commit);
    }
);

simulation_test!(
    create_version_with_options_and_switch_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        let created = engine
            .create_version(CreateVersionOptions {
                id: Some("branch-alpha".to_string()),
                name: Some("Branch Alpha".to_string()),
                hidden: true,
            })
            .await
            .expect("create_version should succeed");
        assert_eq!(created.id, "branch-alpha");
        assert_eq!(created.name, "Branch Alpha");

        let created_row = engine
            .execute(
                "SELECT id, name, hidden \
                 FROM lix_version \
                 WHERE id = 'branch-alpha'",
                &[],
            )
            .await
            .expect("created version query should succeed");
        assert_eq!(created_row.statements[0].rows.len(), 1);
        let row = &created_row.statements[0].rows[0];
        assert_eq!(value_as_text(&row[0]), "branch-alpha");
        assert_eq!(value_as_text(&row[1]), "Branch Alpha");
        assert!(value_as_bool(&row[2]));

        engine
            .switch_version("branch-alpha".to_string())
            .await
            .expect("switch_version should succeed");
        let active = engine
            .execute(
                "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
                &[],
            )
            .await
            .expect("active version query should succeed");
        assert_eq!(
            value_as_text(&active.statements[0].rows[0][0]),
            "branch-alpha"
        );
    }
);

simulation_test!(switch_version_rejects_invalid_inputs, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    let empty = engine
        .switch_version("".to_string())
        .await
        .expect_err("empty version id should fail");
    assert!(empty.description.contains("non-empty"));

    let missing = engine
        .switch_version("missing-version-id".to_string())
        .await
        .expect_err("unknown version id should fail");
    assert!(missing.description.contains("does not exist"));
});

simulation_test!(create_version_switch_then_checkpoint, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    let created = engine
        .create_version(CreateVersionOptions::default())
        .await
        .expect("create_version should succeed");

    engine
        .switch_version(created.id)
        .await
        .expect("switch_version should succeed");

    engine
        .create_checkpoint()
        .await
        .expect("create_checkpoint should succeed after switching to created version");
});

simulation_test!(merge_version_fast_forwards_target, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-source-ff".to_string()),
            ..Default::default()
        })
        .await
        .expect("source version should be created");
    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-target-ff".to_string()),
            ..Default::default()
        })
        .await
        .expect("target version should be created");

    engine
        .switch_version("merge-source-ff".to_string())
        .await
        .expect("switch to source should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-ff', 'source')",
            &[],
        )
        .await
        .expect("source branch write should succeed");

    let source_head_before = version_commit_id(&engine, "merge-source-ff").await;
    let target_head_before = version_commit_id(&engine, "merge-target-ff").await;

    let merged = engine
        .merge_version(MergeVersionOptions {
            source_version_id: "merge-source-ff".to_string(),
            target_version_id: "merge-target-ff".to_string(),
            expected_heads: None,
        })
        .await
        .expect("merge_version should fast-forward target");

    assert_eq!(merged.outcome, MergeOutcome::FastForwarded);
    assert_eq!(merged.source_head_before_commit_id, source_head_before);
    assert_eq!(merged.target_head_before_commit_id, target_head_before);
    assert_eq!(merged.target_head_after_commit_id, source_head_before);
    assert_eq!(merged.created_merge_commit_id, None);
    assert_eq!(merged.applied_change_count, 0);
    assert_eq!(merged.created_tombstone_count, 0);

    engine
        .switch_version("merge-target-ff".to_string())
        .await
        .expect("switch to fast-forwarded target should succeed");
    assert_eq!(
        key_value_value(&engine, "merge-version-ff")
            .await
            .as_deref(),
        Some("source")
    );
});

simulation_test!(
    merge_version_creates_merge_commit_for_diverged_versions,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.expect("init should succeed");

        engine
            .create_version(CreateVersionOptions {
                id: Some("merge-source-diverged".to_string()),
                ..Default::default()
            })
            .await
            .expect("source version should be created");
        engine
            .create_version(CreateVersionOptions {
                id: Some("merge-target-diverged".to_string()),
                ..Default::default()
            })
            .await
            .expect("target version should be created");

        engine
            .switch_version("merge-source-diverged".to_string())
            .await
            .expect("switch to source should succeed");
        engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-source-only', 'source')",
            &[],
        )
        .await
        .expect("source branch write should succeed");
        let source_head_before = version_commit_id(&engine, "merge-source-diverged").await;

        engine
            .switch_version("merge-target-diverged".to_string())
            .await
            .expect("switch to target should succeed");
        engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-target-only', 'target')",
            &[],
        )
        .await
        .expect("target branch write should succeed");
        let target_head_before = version_commit_id(&engine, "merge-target-diverged").await;

        let merged = engine
            .merge_version(MergeVersionOptions {
                source_version_id: "merge-source-diverged".to_string(),
                target_version_id: "merge-target-diverged".to_string(),
                expected_heads: None,
            })
            .await
            .expect("merge_version should create a merge commit");

        assert_eq!(merged.outcome, MergeOutcome::MergeCommitted);
        assert_eq!(merged.source_head_before_commit_id, source_head_before);
        assert_eq!(merged.target_head_before_commit_id, target_head_before);
        assert_eq!(merged.applied_change_count, 1);
        assert_eq!(merged.created_tombstone_count, 0);
        let merge_commit_id = merged
            .created_merge_commit_id
            .clone()
            .expect("merge commit id should exist");
        assert_eq!(merged.target_head_after_commit_id, merge_commit_id);
        assert_ne!(merge_commit_id, source_head_before);
        assert_ne!(merge_commit_id, target_head_before);

        let parent_ids = merge_commit_parent_ids(&engine, &merge_commit_id).await;
        assert_eq!(
            parent_ids,
            vec![source_head_before.clone(), target_head_before.clone()]
        );

        assert_eq!(
            version_commit_id(&engine, "merge-source-diverged").await,
            source_head_before
        );
        assert_eq!(
            version_commit_id(&engine, "merge-target-diverged").await,
            merge_commit_id
        );

        assert_eq!(
            key_value_value(&engine, "merge-version-source-only")
                .await
                .as_deref(),
            Some("source")
        );
        assert_eq!(
            key_value_value(&engine, "merge-version-target-only")
                .await
                .as_deref(),
            Some("target")
        );

        engine
            .switch_version("merge-target-diverged".to_string())
            .await
            .expect("switch to merged target should succeed");
        assert_eq!(
            key_value_value(&engine, "merge-version-source-only")
                .await
                .as_deref(),
            Some("source")
        );
        assert_eq!(
            key_value_value(&engine, "merge-version-target-only")
                .await
                .as_deref(),
            Some("target")
        );

        engine
            .switch_version("merge-source-diverged".to_string())
            .await
            .expect("switch back to source should succeed");
        assert_eq!(
            key_value_value(&engine, "merge-version-source-only")
                .await
                .as_deref(),
            Some("source")
        );
        assert_eq!(
            key_value_value(&engine, "merge-version-target-only")
                .await
                .as_deref(),
            None
        );
    }
);

simulation_test!(merge_version_rejects_entity_conflicts, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-source-conflict".to_string()),
            ..Default::default()
        })
        .await
        .expect("source version should be created");
    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-target-conflict".to_string()),
            ..Default::default()
        })
        .await
        .expect("target version should be created");

    engine
        .switch_version("merge-source-conflict".to_string())
        .await
        .expect("switch to source should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-conflict', 'source')",
            &[],
        )
        .await
        .expect("source insert should succeed");

    engine
        .switch_version("merge-target-conflict".to_string())
        .await
        .expect("switch to target should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-conflict', 'target')",
            &[],
        )
        .await
        .expect("target insert should succeed");
    let target_head_before = version_commit_id(&engine, "merge-target-conflict").await;

    let error = engine
        .merge_version(MergeVersionOptions {
            source_version_id: "merge-source-conflict".to_string(),
            target_version_id: "merge-target-conflict".to_string(),
            expected_heads: None,
        })
        .await
        .expect_err("merge_version should fail on conflicting entity changes");

    assert_eq!(error.code, "LIX_ERROR_MERGE_CONFLICT");
    assert!(error.description.contains("merge-version-conflict"));
    assert_eq!(
        version_commit_id(&engine, "merge-target-conflict").await,
        target_head_before
    );
    assert_eq!(
        key_value_value(&engine, "merge-version-conflict")
            .await
            .as_deref(),
        Some("target")
    );
});
