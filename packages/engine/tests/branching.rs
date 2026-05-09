#[macro_use]
#[path = "support/mod.rs"]
mod support;

use lix_engine::Value;
use lix_engine::{
    CreateVersionOptions, Engine, LixError, MergeChangeStats, MergeVersionOptions,
    MergeVersionOutcome, MergeVersionPreviewOptions, SwitchVersionOptions,
};
use serde_json::Value as JsonValue;

simulation_test!(create_version_from_main, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    assert_version_descriptor(&main, "draft-version", "Draft").await;
    assert_eq!(
        engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load"),
        Some(sim.initial_commit_id().to_string())
    );

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(create_version_rejects_existing_id, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    let error = main
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Overwritten draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect_err("creating a version with an existing id should fail");

    assert_eq!(error.code, "LIX_ERROR_UNIQUE");
    assert!(
        error
            .to_string()
            .contains("INSERT would duplicate entity_id"),
        "error should explain the duplicate version id: {error:?}"
    );
    assert_version_descriptor(&main, "draft-version", "Draft").await;

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(create_version_rejects_duplicate_name, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    let error = main
        .create_version(CreateVersionOptions {
            id: Some("duplicate-name-version".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect_err("creating a version with an existing name should fail");

    assert_eq!(error.code, lix_engine::LixError::CODE_UNIQUE);
    assert!(
        error.to_string().contains("/name"),
        "error should explain the duplicate version name: {error:?}"
    );

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(
    version_descriptor_delete_via_entity_surface_is_rejected_when_ref_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
            .execute(
                "DELETE FROM lix_version_descriptor WHERE id = 'draft-version'",
                &[],
            )
            .await
            .expect_err("descriptor delete through entity surface should fail");
        assert_version_pair_delete_restricted(&error);

        assert_eq!(count_version_descriptors(&main, "draft-version").await, 1);
        assert_eq!(count_version_refs(&main, "draft-version").await, 1);
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("version ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    version_descriptor_delete_via_lix_state_is_rejected_when_ref_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
		.execute(
			"DELETE FROM lix_state \
	             WHERE schema_key = 'lix_version_descriptor' AND entity_id = lix_json('[\"draft-version\"]')",
			&[],
		)
            .await
            .expect_err("descriptor delete through lix_state should fail");
        assert_version_pair_delete_restricted(&error);

        assert_eq!(count_version_descriptors(&main, "draft-version").await, 1);
        assert_eq!(count_version_refs(&main, "draft-version").await, 1);
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("version ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    version_ref_delete_via_entity_surface_is_rejected_when_descriptor_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
            .execute(
                "DELETE FROM lix_version_ref WHERE id = 'draft-version'",
                &[],
            )
            .await
            .expect_err("ref delete through entity surface should fail");
        assert_version_pair_delete_restricted(&error);

        assert_eq!(count_version_descriptors(&main, "draft-version").await, 1);
        assert_eq!(count_version_refs(&main, "draft-version").await, 1);
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("version ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    version_ref_delete_via_lix_state_is_rejected_when_descriptor_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
		.execute(
			"DELETE FROM lix_state \
	                 WHERE schema_key = 'lix_version_ref' AND entity_id = lix_json('[\"draft-version\"]')",
			&[],
		)
            .await
            .expect_err("ref delete through lix_state should fail");
        assert_version_pair_delete_restricted(&error);

        assert_eq!(count_version_descriptors(&main, "draft-version").await, 1);
        assert_eq!(count_version_refs(&main, "draft-version").await, 1);
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("version ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    create_version_can_start_from_explicit_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main-after-initial', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");

        assert_key_value(&main, "main-after-initial", Some("\"main\"")).await;

        let receipt = main
            .create_version(CreateVersionOptions {
                id: Some("from-initial".to_string()),
                name: "From initial".to_string(),
                from_commit_id: Some(sim.initial_commit_id().to_string()),
            })
            .await
            .expect("version should be created from explicit commit");
        assert_eq!(receipt.id, "from-initial");
        assert_eq!(receipt.name, "From initial");
        assert!(!receipt.hidden);
        assert_eq!(receipt.commit_id, sim.initial_commit_id());
        assert_eq!(
            engine
                .load_version_head_commit_id("from-initial")
                .await
                .expect("version head should load"),
            Some(sim.initial_commit_id().to_string())
        );

        let from_initial = main.wrap_session(
            engine
                .open_session("from-initial")
                .await
                .expect("explicit commit version session should open"),
            &engine,
        );
        assert_key_value(&from_initial, "main-after-initial", None).await;

        drop(from_initial);
        drop(main);
        drop(engine);
    }
);

simulation_test!(created_version_sees_inherited_state, |sim| async move {
    let (_engine, _main, draft) = create_draft_after_shared_write(&sim).await;

    assert_key_value(&draft, "shared-before-branch", Some("\"shared\"")).await;
});

simulation_test!(
    open_workspace_session_starts_on_seeded_main_version,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let workspace = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        assert_eq!(
            workspace
                .active_version_id()
                .await
                .expect("workspace active version should resolve"),
            sim.main_version_id()
        );
    }
);

simulation_test!(
    later_main_changes_do_not_appear_in_created_version,
    |sim| async move {
        let (_engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main-after-branch', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");

        assert_key_value(&main, "main-after-branch", Some("\"main\"")).await;
        assert_key_value(&draft, "main-after-branch", None).await;
    }
);

simulation_test!(
    later_created_version_changes_do_not_appear_in_main,
    |sim| async move {
        let (_engine, main, draft) = create_draft_from_main(&sim).await;

        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('draft-after-branch', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        assert_key_value(&draft, "draft-after-branch", Some("\"draft\"")).await;
        assert_key_value(&main, "draft-after-branch", None).await;
    }
);

simulation_test!(
    switch_version_returns_session_for_target_version,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('switch-draft-only', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let (switched, receipt) = main
            .switch_version(SwitchVersionOptions {
                version_id: "draft-version".to_string(),
            })
            .await
            .expect("switch should succeed");

        assert_eq!(receipt.version_id, "draft-version");
        assert_key_value(&switched, "switch-draft-only", Some("\"draft\"")).await;
        assert_key_value(&main, "switch-draft-only", None).await;

        drop(engine);
    }
);

simulation_test!(
    pinned_switch_version_is_ephemeral_and_does_not_advance_refs,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load");
        let draft_head_before = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load");
        let workspace_before = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        assert_eq!(
            workspace_before
                .active_version_id()
                .await
                .expect("workspace selector should resolve"),
            sim.main_version_id(),
            "pinned session setup should not have moved the workspace selector"
        );

        let (_switched, _receipt) = main
            .switch_version(SwitchVersionOptions {
                version_id: "draft-version".to_string(),
            })
            .await
            .expect("switch should succeed");

        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            main_head_before,
            "switching must not mutate the source session version ref"
        );
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("draft head should load"),
            draft_head_before,
            "switching must not mutate the target version ref"
        );
        let workspace_after = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        assert_eq!(
            workspace_after
                .active_version_id()
                .await
                .expect("workspace selector should resolve"),
            sim.main_version_id(),
            "pinned switching must not mutate the shared workspace selector"
        );
    }
);

simulation_test!(
    workspace_switch_version_updates_shared_workspace_selector,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('workspace-draft-only', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load");
        let draft_head_before = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load");

        let workspace_a = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        let workspace_b = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("second workspace session should open"),
            &engine,
        );
        assert_eq!(
            workspace_a
                .active_version_id()
                .await
                .expect("workspace selector should resolve"),
            sim.main_version_id()
        );

        let (workspace_switched, receipt) = workspace_a
            .switch_version(SwitchVersionOptions {
                version_id: "draft-version".to_string(),
            })
            .await
            .expect("workspace switch should succeed");

        assert_eq!(receipt.version_id, "draft-version");
        assert_eq!(
            workspace_switched
                .active_version_id()
                .await
                .expect("switched workspace selector should resolve"),
            "draft-version"
        );
        assert_eq!(
            workspace_b
                .active_version_id()
                .await
                .expect("other workspace session should observe selector"),
            "draft-version",
            "workspace sessions resolve the shared selector on use"
        );
        assert_key_value(&workspace_b, "workspace-draft-only", Some("\"draft\"")).await;
        assert_key_value(&main, "workspace-draft-only", None).await;
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            main_head_before,
            "workspace switching must not mutate the old version ref"
        );
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("draft head should load"),
            draft_head_before,
            "workspace switching must not mutate the new version ref"
        );
    }
);

simulation_test!(
    workspace_switch_version_persists_across_reopened_engine,
    |sim| async move {
        let (engine, _main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('workspace-reopen-draft', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let workspace = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        workspace
            .switch_version(SwitchVersionOptions {
                version_id: "draft-version".to_string(),
            })
            .await
            .expect("workspace switch should persist");

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reopen from current snapshot");
        let reopened_workspace = sim.wrap_session(
            reopened_engine
                .open_workspace_session()
                .await
                .expect("reopened workspace session should open"),
            &reopened_engine,
        );

        assert_eq!(
            reopened_workspace
                .active_version_id()
                .await
                .expect("workspace selector should resolve after reopen"),
            "draft-version",
            "workspace switch should survive reopening the engine"
        );
        assert_key_value(
            &reopened_workspace,
            "workspace-reopen-draft",
            Some("\"draft\""),
        )
        .await;
    }
);

simulation_test!(
    switch_version_errors_when_target_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = main
            .switch_version(SwitchVersionOptions {
                version_id: "missing-version".to_string(),
            })
            .await;
        let Err(error) = result else {
            panic!("missing version ref should fail");
        };

        assert_eq!(error.code, LixError::CODE_VERSION_NOT_FOUND);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("version_id")),
            Some(&JsonValue::String("missing-version".to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("operation")),
            Some(&JsonValue::String("switch_version".to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("role")),
            Some(&JsonValue::String("target".to_string()))
        );
    }
);

simulation_test!(
    merge_version_resolves_existing_source_and_target_heads,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge head resolution should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::AlreadyUpToDate);
        assert_eq!(receipt.change_stats, MergeChangeStats::default());
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(receipt.target_version_id, sim.main_version_id());
        assert_eq!(receipt.source_version_id, "draft-version");
        assert_eq!(
            receipt.target_head_before_commit_id, main_head_before,
            "receipt should expose the target head before the no-op merge"
        );
        assert_eq!(
            receipt.target_head_after_commit_id, main_head_before,
            "no-op merge should leave target head unchanged"
        );
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before)
        );
    }
);

simulation_test!(
    merge_version_fast_forwards_when_target_is_merge_base,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('draft-fast-forward', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let target_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let preview = main
            .merge_version_preview(MergeVersionPreviewOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge preview should analyze fast-forward");
        assert_eq!(preview.outcome, MergeVersionOutcome::FastForward);
        assert_eq!(preview.target_head_commit_id, target_head_before);
        assert_eq!(preview.source_head_commit_id, source_head);
        assert_eq!(
            preview.change_stats,
            MergeChangeStats {
                total: 1,
                added: 1,
                modified: 0,
                removed: 0,
            }
        );
        assert_eq!(preview.conflicts.len(), 0);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load")
                .as_deref(),
            Some(target_head_before.as_str()),
            "preview should not advance the target ref"
        );

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge should fast-forward target");
        assert_eq!(receipt.outcome, MergeVersionOutcome::FastForward);
        assert_eq!(
            receipt.change_stats,
            MergeChangeStats {
                total: 1,
                added: 1,
                modified: 0,
                removed: 0,
            }
        );
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(receipt.base_commit_id, target_head_before);
        assert_eq!(receipt.target_head_before_commit_id, target_head_before);
        assert_eq!(receipt.source_head_before_commit_id, source_head);
        assert_eq!(receipt.target_head_after_commit_id, source_head);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load")
                .as_deref(),
            Some(source_head.as_str())
        );
        assert_key_value(&main, "draft-fast-forward", Some("\"draft\"")).await;

        let global = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        assert_eq!(
            commit_parent_edges(&global, &source_head).await,
            vec![(target_head_before, 0)],
            "fast-forward should not create a two-parent merge commit"
        );
    }
);

simulation_test!(
    merge_version_advances_target_with_two_parent_commit,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main-merge-target', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('draft-merge-source', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let target_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge should apply source change");
        assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
        assert_eq!(
            receipt.change_stats,
            MergeChangeStats {
                total: 1,
                added: 1,
                modified: 0,
                removed: 0,
            }
        );
        assert_eq!(receipt.target_head_before_commit_id, target_head_before);
        assert_eq!(receipt.source_head_before_commit_id, source_head);

        let target_head_after = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        assert_eq!(
            receipt.target_head_after_commit_id, target_head_after,
            "receipt should expose the post-merge target head"
        );
        assert_eq!(
            receipt.created_merge_commit_id.as_deref(),
            Some(target_head_after.as_str()),
            "a non-empty merge should report the merge commit it created"
        );
        assert_ne!(target_head_after, target_head_before);
        assert_eq!(
            engine
                .load_version_head_commit_id("draft-version")
                .await
                .expect("draft head should load")
                .as_deref(),
            Some(source_head.as_str()),
            "merging into main must not move the source version ref"
        );

        assert_key_value(&main, "draft-merge-source", Some("\"draft\"")).await;
        assert_key_value(&main, "main-merge-target", Some("\"main\"")).await;

        let global = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        assert_eq!(
            commit_parent_edges(&global, &target_head_after).await,
            vec![(target_head_before, 0), (source_head, 1)],
            "merge commit should preserve target as first parent and source as second parent"
        );
    }
);

simulation_test!(
    merge_version_adopts_source_change_without_minting_equivalent_copy,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-adopt-target', 'target')",
            &[],
        )
        .await
        .expect("main write should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-adopt-change', 'source')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge should apply source change");
        assert!(
            receipt.created_merge_commit_id.is_some(),
            "non-empty merge should create a merge commit"
        );

        let global = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        let equivalent_change_count = select_single_integer(
            &global,
            "SELECT count(*) \
	     FROM lix_change \
	     WHERE schema_key = 'lix_key_value' \
	       AND entity_id = lix_json('[\"merge-adopt-change\"]') \
	       AND snapshot_content = lix_json('{\"key\":\"merge-adopt-change\",\"value\":\"source\"}')",
        )
        .await;
        assert_eq!(
            equivalent_change_count, 1,
            "merge must not append a second canonical change with identical effect"
        );

        let history = main
            .execute(
                "SELECT snapshot_content \
	             FROM lix_state_history \
	             WHERE start_commit_id = lix_active_version_commit_id() \
	               AND entity_id = lix_json('[\"merge-adopt-change\"]') \
	             ORDER BY depth",
                &[],
            )
            .await
            .expect("history query should succeed");
        assert_eq!(
            history.len(),
            1,
            "history should show the adopted canonical change once, not once from the merge commit and once from the source parent"
        );
    }
);

simulation_test!(
    merge_version_adopts_schema_registration_before_schema_rows,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-schema-target-change', 'target')",
            &[],
        )
        .await
        .expect("main write should force a merge commit instead of fast-forward");

        draft
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (\
                 lix_json('{\"x-lix-key\":\"merge_task_item\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"title\":{\"type\":\"string\"}},\"required\":[\"id\",\"title\"],\"additionalProperties\":false}')\
                 )",
                &[],
            )
            .await
            .expect("draft schema registration should succeed");

        draft
            .execute(
                "INSERT INTO merge_task_item (id, title) \
                 VALUES ('task-1', 'Adopted schema row')",
                &[],
            )
            .await
            .expect("draft row using newly registered schema should succeed");

        main.merge_version(MergeVersionOptions {
            source_version_id: "draft-version".to_string(),
        })
        .await
        .expect("merge should adopt schema registration before rows that use it");

        let reopened_main = sim.wrap_session(
            engine
                .open_session(sim.main_version_id())
                .await
                .expect("main session should reopen after merge"),
            &engine,
        );

        let rows = reopened_main
            .execute(
                "SELECT id, title FROM merge_task_item WHERE id = 'task-1'",
                &[],
            )
            .await
            .expect("merged schema surface should be queryable");
        assert_eq!(
            rows.rows()[0].values(),
            &[
                Value::Text("task-1".to_string()),
                Value::Text("Adopted schema row".to_string()),
            ]
        );
    }
);

simulation_test!(
    merge_version_errors_on_divergent_same_entity_change,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-conflict', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-conflict', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect_err("divergent same-entity changes should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target version ref"
        );
        assert_key_value(&main, "merge-conflict", Some("\"main\"")).await;
    }
);

simulation_test!(
    merge_version_fast_forwards_source_delete_when_target_unchanged,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&draft, "shared-before-branch").await;
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge should apply source delete");

        assert_eq!(receipt.outcome, MergeVersionOutcome::FastForward);
        assert_eq!(
            receipt.change_stats,
            MergeChangeStats {
                total: 1,
                added: 0,
                modified: 0,
                removed: 1,
            }
        );
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(receipt.target_head_after_commit_id, source_head);
        assert_key_value(&main, "shared-before-branch", None).await;
    }
);

simulation_test!(
    merge_version_records_empty_merge_when_both_sides_delete,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&main, "shared-before-branch").await;
        delete_key_value(&draft, "shared-before-branch").await;
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("convergent delete merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
        assert_eq!(receipt.change_stats, MergeChangeStats::default());
        let merge_commit_id = receipt
            .created_merge_commit_id
            .clone()
            .expect("convergent delete should create an empty merge commit");
        assert_eq!(receipt.target_head_after_commit_id, merge_commit_id);
        assert_eq!(receipt.target_head_before_commit_id, main_head_before);
        assert_eq!(receipt.source_head_before_commit_id, source_head);
        assert_empty_merge_commit(
            &engine,
            &main,
            &merge_commit_id,
            &receipt.target_head_before_commit_id,
            &receipt.source_head_before_commit_id,
        )
        .await;
        assert_key_value(&main, "shared-before-branch", None).await;
    }
);

simulation_test!(
    merge_version_conflicts_when_target_deletes_source_modifies,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&main, "shared-before-branch").await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'draft' WHERE key = 'shared-before-branch'",
                &[],
            )
            .await
            .expect("draft update should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect_err("delete/modify should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target version ref"
        );
        assert_key_value(&main, "shared-before-branch", None).await;
    }
);

simulation_test!(
    merge_version_conflicts_when_target_modifies_source_deletes,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        main.execute(
            "UPDATE lix_key_value SET value = 'main' WHERE key = 'shared-before-branch'",
            &[],
        )
        .await
        .expect("main update should succeed");
        delete_key_value(&draft, "shared-before-branch").await;
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect_err("modify/delete should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target version ref"
        );
        assert_key_value(&main, "shared-before-branch", Some("\"main\"")).await;
    }
);

simulation_test!(
    merge_version_records_empty_merge_for_same_payload_convergence,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        main.execute(
            "UPDATE lix_key_value SET value = 'same' WHERE key = 'shared-before-branch'",
            &[],
        )
        .await
        .expect("main update should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'same' WHERE key = 'shared-before-branch'",
                &[],
            )
            .await
            .expect("draft update should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("convergent update merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
        assert_eq!(receipt.change_stats, MergeChangeStats::default());
        let merge_commit_id = receipt
            .created_merge_commit_id
            .clone()
            .expect("convergent update should create an empty merge commit");
        assert_eq!(receipt.target_head_after_commit_id, merge_commit_id);
        assert_eq!(receipt.target_head_before_commit_id, main_head_before);
        assert_eq!(receipt.source_head_before_commit_id, source_head);
        assert_empty_merge_commit(
            &engine,
            &main,
            &merge_commit_id,
            &receipt.target_head_before_commit_id,
            &receipt.source_head_before_commit_id,
        )
        .await;
        assert_key_value(&main, "shared-before-branch", Some("\"same\"")).await;
    }
);

simulation_test!(
    merge_version_conflicts_on_independent_add_same_identity_different_payload,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-independent-add', 'main')",
            &[],
        )
        .await
        .expect("main insert should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-independent-add', 'draft')",
                &[],
            )
            .await
            .expect("draft insert should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect_err("independent adds with different payloads should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target version ref"
        );
        assert_key_value(&main, "merge-independent-add", Some("\"main\"")).await;
    }
);

simulation_test!(
    merge_version_records_empty_merge_for_same_identity_same_payload_add,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-independent-same-add', 'same')",
            &[],
        )
        .await
        .expect("main insert should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-independent-same-add', 'same')",
                &[],
            )
            .await
            .expect("draft insert should succeed");
        let main_head_before = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_version_head_commit_id("draft-version")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("convergent independent add merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
        assert_eq!(receipt.change_stats, MergeChangeStats::default());
        let merge_commit_id = receipt
            .created_merge_commit_id
            .clone()
            .expect("convergent independent add should create an empty merge commit");
        assert_eq!(receipt.target_head_after_commit_id, merge_commit_id);
        assert_eq!(receipt.target_head_before_commit_id, main_head_before);
        assert_eq!(receipt.source_head_before_commit_id, source_head);
        assert_empty_merge_commit(
            &engine,
            &main,
            &merge_commit_id,
            &receipt.target_head_before_commit_id,
            &receipt.source_head_before_commit_id,
        )
        .await;
        assert_key_value(&main, "merge-independent-same-add", Some("\"same\"")).await;
    }
);

simulation_test!(
    merge_version_errors_when_source_version_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = main
            .merge_version(MergeVersionOptions {
                source_version_id: "missing-version".to_string(),
            })
            .await
            .expect_err("missing source ref should fail");

        assert_eq!(error.code, LixError::CODE_VERSION_NOT_FOUND);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("version_id")),
            Some(&JsonValue::String("missing-version".to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("operation")),
            Some(&JsonValue::String("merge_version".to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("role")),
            Some(&JsonValue::String("source".to_string()))
        );
    }
);

simulation_test!(merge_version_rejects_self_merge, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = main
        .merge_version(MergeVersionOptions {
            source_version_id: sim.main_version_id().to_string(),
        })
        .await
        .expect_err("self-merge should fail");

    assert_eq!(error.code, LixError::CODE_INVALID_MERGE);
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("operation")),
        Some(&JsonValue::String("merge_version".to_string()))
    );
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("target_version_id")),
        Some(&JsonValue::String(sim.main_version_id().to_string()))
    );
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("source_version_id")),
        Some(&JsonValue::String(sim.main_version_id().to_string()))
    );
});

async fn delete_key_value(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
) {
    session
        .execute(
            &format!("DELETE FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("key-value delete should succeed");
}

async fn create_draft_after_shared_write(
    sim: &crate::support::simulation_test::engine::Simulation,
) -> (
    Engine,
    crate::support::simulation_test::engine::SimSession,
    crate::support::simulation_test::engine::SimSession,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session(sim.main_version_id())
            .await
            .expect("main session should open"),
        &engine,
    );
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('shared-before-branch', 'shared')",
        &[],
    )
    .await
    .expect("source write should succeed");

    let draft = create_draft(&engine, &main).await;
    (engine, main, draft)
}

async fn create_draft_from_main(
    sim: &crate::support::simulation_test::engine::Simulation,
) -> (
    Engine,
    crate::support::simulation_test::engine::SimSession,
    crate::support::simulation_test::engine::SimSession,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session(sim.main_version_id())
            .await
            .expect("main session should open"),
        &engine,
    );
    let draft = create_draft(&engine, &main).await;
    (engine, main, draft)
}

async fn create_draft(
    engine: &Engine,
    main: &crate::support::simulation_test::engine::SimSession,
) -> crate::support::simulation_test::engine::SimSession {
    let receipt = main
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("version should be created");
    assert_eq!(receipt.id, "draft-version");
    let version_row = main
        .execute(
            "SELECT id, name, hidden, commit_id FROM lix_version WHERE id = 'draft-version'",
            &[],
        )
        .await
        .expect("created version should be queryable through lix_version");
    assert_eq!(version_row.len(), 1);
    assert_eq!(
        version_row.rows()[0].values(),
        &[
            Value::Text(receipt.id.clone()),
            Value::Text(receipt.name.clone()),
            Value::Boolean(receipt.hidden),
            Value::Text(receipt.commit_id.clone()),
        ],
        "create_version should return the same public shape as lix_version"
    );
    main.wrap_session(
        engine
            .open_session(receipt.id)
            .await
            .expect("draft session should open"),
        engine,
    )
}

async fn assert_key_value(
    session: &crate::support::simulation_test::engine::SimSession,
    key: &str,
    expected: Option<&str>,
) {
    let result = session
        .execute(
            &format!("SELECT value FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("key-value query should succeed");
    let rows = result;
    match expected {
        Some(value) => {
            assert_eq!(rows.len(), 1);
            let expected_json = serde_json::from_str::<JsonValue>(value)
                .expect("expected key-value should be valid JSON");
            assert_eq!(rows.rows()[0].values(), &[Value::Json(expected_json)]);
        }
        None => assert_eq!(rows.len(), 0),
    }
}

async fn assert_version_descriptor(
    session: &crate::support::simulation_test::engine::SimSession,
    version_id: &str,
    expected_name: &str,
) {
    let result = session
        .execute(
            &format!("SELECT id, name FROM lix_version WHERE id = '{version_id}'"),
            &[],
        )
        .await
        .expect("version query should succeed");
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text(version_id.to_string()),
            Value::Text(expected_name.to_string()),
        ]
    );
}

async fn count_version_descriptors(
    session: &crate::support::simulation_test::engine::SimSession,
    version_id: &str,
) -> i64 {
    select_single_integer(
        session,
        &format!("SELECT COUNT(*) FROM lix_version_descriptor WHERE id = '{version_id}'"),
    )
    .await
}

async fn count_version_refs(
    session: &crate::support::simulation_test::engine::SimSession,
    version_id: &str,
) -> i64 {
    select_single_integer(
        session,
        &format!(
            "SELECT COUNT(*) FROM lix_state \
	         WHERE schema_key = 'lix_version_ref' AND entity_id = lix_json('[\"{version_id}\"]')"
        ),
    )
    .await
}

fn assert_version_pair_delete_restricted(error: &lix_engine::LixError) {
    assert_eq!(error.code, lix_engine::LixError::CODE_READ_ONLY);
    assert!(
        error.to_string().contains("lix_version"),
        "error should explain the version pair restriction: {error:?}"
    );
    assert!(
        error
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("lix_version")),
        "error should guide callers to the lix_version surface: {error:?}"
    );
}

fn assert_merge_conflict_error(error: &lix_engine::LixError) {
    assert_eq!(error.code, "LIX_MERGE_CONFLICT");
    assert!(
        error.message.contains("tracked-state conflict"),
        "unexpected merge error: {error:?}"
    );
    let details = error
        .details
        .as_ref()
        .expect("merge conflict should include details");
    let conflicts = details
        .get("conflicts")
        .and_then(JsonValue::as_array)
        .expect("merge conflict details should include conflicts array");
    assert_eq!(conflicts.len(), 1);
    let conflict = &conflicts[0];
    assert_eq!(
        conflict.get("kind").and_then(JsonValue::as_str),
        Some("sameEntityChanged")
    );
    assert_eq!(
        conflict.get("schemaKey").and_then(JsonValue::as_str),
        Some("lix_key_value")
    );
    assert!(
        conflict
            .get("entityId")
            .and_then(JsonValue::as_array)
            .is_some(),
        "conflict should include entityId: {conflict:?}"
    );
    assert!(
        conflict.get("target").is_some(),
        "conflict should include target side: {conflict:?}"
    );
    assert!(
        conflict.get("source").is_some(),
        "conflict should include source side: {conflict:?}"
    );
}

async fn select_single_integer(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> i64 {
    let result = session
        .execute(sql, &[])
        .await
        .expect("query should succeed");
    assert_eq!(result.len(), 1, "expected exactly one row for query: {sql}");
    let Value::Integer(value) = result.rows()[0].values()[0] else {
        panic!("expected integer value for query: {sql}");
    };
    value
}

async fn commit_parent_edges(
    session: &crate::support::simulation_test::engine::SimSession,
    commit_id: &str,
) -> Vec<(String, i64)> {
    let result = session
        .execute(
            &format!(
                "SELECT parent_id, parent_order \
                 FROM lix_commit_edge \
                 WHERE child_id = '{commit_id}' \
                 ORDER BY parent_order"
            ),
            &[],
        )
        .await
        .expect("commit edges should read");
    result
        .rows()
        .iter()
        .map(|row| {
            let Value::Text(value) = &row.values()[0] else {
                panic!("parent_id should be text");
            };
            let Value::Integer(parent_order) = row.values()[1] else {
                panic!("parent_order should be integer");
            };
            (value.clone(), parent_order)
        })
        .collect()
}

async fn assert_empty_merge_commit(
    engine: &Engine,
    session: &crate::support::simulation_test::engine::SimSession,
    merge_commit_id: &str,
    target_head_before: &str,
    source_head: &str,
) {
    let active_version_id = session
        .active_version_id()
        .await
        .expect("active version should load");
    assert_eq!(
        engine
            .load_version_head_commit_id(&active_version_id)
            .await
            .expect("target version head should load")
            .as_deref(),
        Some(merge_commit_id),
        "empty merge should advance the target version ref"
    );

    let global = session.wrap_session(
        engine
            .open_session("global")
            .await
            .expect("global session should open"),
        engine,
    );
    assert_eq!(
        commit_parent_edges(&global, merge_commit_id)
            .await
            .into_iter()
            .map(|(parent_id, _)| parent_id)
            .collect::<std::collections::BTreeSet<_>>(),
        [target_head_before.to_string(), source_head.to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        "empty merge commit should preserve target/source ancestry"
    );
}
