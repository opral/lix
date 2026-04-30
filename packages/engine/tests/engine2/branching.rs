use crate::simulation_test2;
use lix_engine::engine2::{
    CreateVersionOptions, Engine, ExecuteResult, MergeVersionOptions, MergeVersionOutcome,
    SwitchVersionOptions,
};
use lix_engine::Value;
use serde_json::Value as JsonValue;

simulation_test2!(create_version_from_main, |sim| async move {
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

simulation_test2!(created_version_sees_inherited_state, |sim| async move {
    let (_engine, _main, draft) = create_draft_after_shared_write(&sim).await;

    assert_key_value(&draft, "shared-before-branch", Some("\"shared\"")).await;
});

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

simulation_test2!(
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

        assert!(error
            .description
            .contains("cannot switch to missing version ref 'missing-version'"));
    }
);

simulation_test2!(
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
        assert_eq!(receipt.applied_change_count, 0);
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

simulation_test2!(
    merge_version_advances_target_with_two_parent_commit,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
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
        assert_eq!(receipt.applied_change_count, 1);
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

        let global = sim.wrap_session(
            engine
                .open_session("global")
                .await
                .expect("global session should open"),
            &engine,
        );
        let commit_snapshot = load_commit_snapshot(&global, &target_head_after).await;
        let parent_commit_ids = commit_snapshot
            .get("parent_commit_ids")
            .and_then(JsonValue::as_array)
            .expect("merge commit should declare parent_commit_ids")
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .expect("parent commit id should be text")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            parent_commit_ids,
            vec![target_head_before, source_head],
            "merge commit should parent first to the old target head, then to the source head"
        );
    }
);

simulation_test2!(
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
        assert!(
            error.description.contains("tracked-state conflict"),
            "unexpected merge error: {error:?}"
        );
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

simulation_test2!(
    merge_version_applies_source_delete_when_target_unchanged,
    |sim| async move {
        let (_engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&draft, "shared-before-branch").await;

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("merge should apply source delete");

        assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
        assert_eq!(receipt.applied_change_count, 1);
        assert!(receipt.created_merge_commit_id.is_some());
        assert_key_value(&main, "shared-before-branch", None).await;
    }
);

simulation_test2!(
    merge_version_treats_both_sides_delete_as_noop,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&main, "shared-before-branch").await;
        delete_key_value(&draft, "shared-before-branch").await;
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
            .expect("convergent delete merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::AlreadyUpToDate);
        assert_eq!(receipt.applied_change_count, 0);
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "convergent delete should not create a new target commit"
        );
        assert_key_value(&main, "shared-before-branch", None).await;
    }
);

simulation_test2!(
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

        assert!(
            error.description.contains("tracked-state conflict"),
            "unexpected merge error: {error:?}"
        );
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

simulation_test2!(
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

        assert!(
            error.description.contains("tracked-state conflict"),
            "unexpected merge error: {error:?}"
        );
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

simulation_test2!(
    merge_version_converges_same_payload_without_new_commit,
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

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("convergent update merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::AlreadyUpToDate);
        assert_eq!(receipt.applied_change_count, 0);
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "convergent update should not create a new target commit"
        );
        assert_key_value(&main, "shared-before-branch", Some("\"same\"")).await;
    }
);

simulation_test2!(
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

        assert!(
            error.description.contains("tracked-state conflict"),
            "unexpected merge error: {error:?}"
        );
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

simulation_test2!(
    merge_version_converges_independent_add_same_identity_same_payload,
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

        let receipt = main
            .merge_version(MergeVersionOptions {
                source_version_id: "draft-version".to_string(),
            })
            .await
            .expect("convergent independent add merge should succeed");

        assert_eq!(receipt.outcome, MergeVersionOutcome::AlreadyUpToDate);
        assert_eq!(receipt.applied_change_count, 0);
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(
            engine
                .load_version_head_commit_id(sim.main_version_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "convergent independent add should not create a new target commit"
        );
        assert_key_value(&main, "merge-independent-same-add", Some("\"same\"")).await;
    }
);

simulation_test2!(
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

        assert!(error
            .description
            .contains("cannot merge from missing source version ref 'missing-version'"));
    }
);

async fn delete_key_value(
    session: &crate::support::simulation_test::engine2::SimSession,
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
    sim: &crate::support::simulation_test::engine2::Engine2Simulation,
) -> (
    Engine,
    crate::support::simulation_test::engine2::SimSession,
    crate::support::simulation_test::engine2::SimSession,
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
    sim: &crate::support::simulation_test::engine2::Engine2Simulation,
) -> (
    Engine,
    crate::support::simulation_test::engine2::SimSession,
    crate::support::simulation_test::engine2::SimSession,
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
    main: &crate::support::simulation_test::engine2::SimSession,
) -> crate::support::simulation_test::engine2::SimSession {
    let receipt = main
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Draft".to_string(),
        })
        .await
        .expect("version should be created");
    assert_eq!(receipt.version_id, "draft-version");
    main.wrap_session(
        engine
            .open_session(receipt.version_id)
            .await
            .expect("draft session should open"),
        engine,
    )
}

async fn assert_key_value(
    session: &crate::support::simulation_test::engine2::SimSession,
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
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    match expected {
        Some(value) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows.rows()[0].values(), &[Value::Text(value.to_string())]);
        }
        None => assert_eq!(rows.len(), 0),
    }
}

async fn assert_version_descriptor(
    session: &crate::support::simulation_test::engine2::SimSession,
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
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text(version_id.to_string()),
            Value::Text(expected_name.to_string()),
        ]
    );
}

async fn load_commit_snapshot(
    session: &crate::support::simulation_test::engine2::SimSession,
    commit_id: &str,
) -> JsonValue {
    let result = session
        .execute(
            &format!(
                "SELECT snapshot_content \
                 FROM lix_state \
                 WHERE schema_key = 'lix_commit' AND entity_id = '{commit_id}'"
            ),
            &[],
        )
        .await
        .expect("commit row should read");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 1);
    let Value::Text(snapshot_content) = &rows.rows()[0].values()[0] else {
        panic!("commit snapshot should be text");
    };
    serde_json::from_str::<JsonValue>(snapshot_content).expect("commit snapshot should be JSON")
}
