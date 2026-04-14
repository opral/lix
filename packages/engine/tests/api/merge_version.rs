use crate::support;

use lix_engine::{
    CreateVersionOptions, ExpectedVersionHeads, MergeOutcome, MergeVersionOptions, Value,
};

fn value_as_text(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        Value::Integer(value) => value.to_string(),
        other => panic!("expected text-like value, got {other:?}"),
    }
}

async fn version_commit_id(
    engine: &support::simulation_test::SimulatedLix,
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
    engine: &support::simulation_test::SimulatedLix,
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
    engine: &support::simulation_test::SimulatedLix,
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

simulation_test!(merge_version_fast_forwards_target, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
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

simulation_test!(merge_version_reports_already_up_to_date_after_target_catches_up, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-source-up-to-date".to_string()),
            ..Default::default()
        })
        .await
        .expect("source version should be created");
    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-target-up-to-date".to_string()),
            ..Default::default()
        })
        .await
        .expect("target version should be created");

    engine
        .switch_version("merge-source-up-to-date".to_string())
        .await
        .expect("switch to source should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-up-to-date', 'source')",
            &[],
        )
        .await
        .expect("source branch write should succeed");

    let first_merge = engine
        .merge_version(MergeVersionOptions {
            source_version_id: "merge-source-up-to-date".to_string(),
            target_version_id: "merge-target-up-to-date".to_string(),
            expected_heads: None,
        })
        .await
        .expect("first merge_version should succeed");
    assert_eq!(first_merge.outcome, MergeOutcome::FastForwarded);

    let source_head = version_commit_id(&engine, "merge-source-up-to-date").await;
    let target_head = version_commit_id(&engine, "merge-target-up-to-date").await;
    assert_eq!(target_head, source_head);

    let second_merge = engine
        .merge_version(MergeVersionOptions {
            source_version_id: "merge-source-up-to-date".to_string(),
            target_version_id: "merge-target-up-to-date".to_string(),
            expected_heads: None,
        })
        .await
        .expect("second merge_version should report already up to date");

    assert_eq!(second_merge.outcome, MergeOutcome::AlreadyUpToDate);
    assert_eq!(second_merge.source_head_before_commit_id, source_head);
    assert_eq!(second_merge.target_head_before_commit_id, target_head.clone());
    assert_eq!(second_merge.target_head_after_commit_id, target_head);
    assert_eq!(second_merge.created_merge_commit_id, None);
    assert_eq!(second_merge.applied_change_count, 0);
    assert_eq!(second_merge.created_tombstone_count, 0);
});

simulation_test!(
    merge_version_creates_merge_commit_for_diverged_versions,
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
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

simulation_test!(merge_version_rejects_stale_expected_target_head, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
    engine.initialize().await.expect("init should succeed");

    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-source-stale-head".to_string()),
            ..Default::default()
        })
        .await
        .expect("source version should be created");
    engine
        .create_version(CreateVersionOptions {
            id: Some("merge-target-stale-head".to_string()),
            ..Default::default()
        })
        .await
        .expect("target version should be created");

    let source_head_before = version_commit_id(&engine, "merge-source-stale-head").await;
    let target_head_before = version_commit_id(&engine, "merge-target-stale-head").await;

    engine
        .switch_version("merge-target-stale-head".to_string())
        .await
        .expect("switch to target should succeed");
    engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-version-stale-head-target', 'target-newer')",
            &[],
        )
        .await
        .expect("target branch write should succeed");
    let target_head_after_local_write = version_commit_id(&engine, "merge-target-stale-head").await;
    assert_ne!(target_head_after_local_write, target_head_before);

    let error = engine
        .merge_version(MergeVersionOptions {
            source_version_id: "merge-source-stale-head".to_string(),
            target_version_id: "merge-target-stale-head".to_string(),
            expected_heads: Some(ExpectedVersionHeads {
                source_head_commit_id: Some(source_head_before.clone()),
                target_head_commit_id: Some(target_head_before.clone()),
            }),
        })
        .await
        .expect_err("merge_version should reject a stale expected target head");

    assert_eq!(error.code, "LIX_ERROR_UNKNOWN");
    assert!(error.description.contains("expected target version"));
    assert!(error.description.contains(&target_head_before));
    assert!(error.description.contains(&target_head_after_local_write));
    assert_eq!(
        version_commit_id(&engine, "merge-source-stale-head").await,
        source_head_before
    );
    assert_eq!(
        version_commit_id(&engine, "merge-target-stale-head").await,
        target_head_after_local_write
    );
    assert_eq!(
        key_value_value(&engine, "merge-version-stale-head-target")
            .await
            .as_deref(),
        Some("target-newer")
    );
});

simulation_test!(merge_version_rejects_entity_conflicts, |sim| async move {
    let engine = sim
        .boot_simulated_lix(None)
        .await
        .expect("boot_simulated_lix should succeed");
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
    let source_head_before = version_commit_id(&engine, "merge-source-conflict").await;

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
        version_commit_id(&engine, "merge-source-conflict").await,
        source_head_before
    );
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
