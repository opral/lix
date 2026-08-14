use crate::support;
use lix::Value;
use lix::engine::Engine;
use lix::{
    CreateBranchOptions, LixError, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, MergeChangeStats, SwitchBranchOptions,
};
use serde_json::Value as JsonValue;

simulation_test!(create_branch_from_main, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    assert_branch_descriptor(&main, "01930000-0000-7000-8000-000000000001", "Draft").await;
    assert_eq!(
        engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load"),
        Some(sim.initial_commit_id().to_string())
    );

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(create_branch_rejects_existing_id, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    let error = main
        .create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000001".to_string()),
            name: "Overwritten draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect_err("creating a branch with an existing id should fail");

    assert_eq!(error.code, "LIX_ERROR_UNIQUE");
    assert!(
        error
            .to_string()
            .contains("INSERT would duplicate entity_pk"),
        "error should explain the duplicate branch id: {error:?}"
    );
    assert_branch_descriptor(&main, "01930000-0000-7000-8000-000000000001", "Draft").await;

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(create_branch_rejects_duplicate_name, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;

    let error = main
        .create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000002".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect_err("creating a branch with an existing name should fail");

    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error.to_string().contains("/name"),
        "error should explain the duplicate branch name: {error:?}"
    );

    drop(draft);
    drop(main);
    drop(engine);
});

simulation_test!(
    branch_descriptor_delete_via_entity_surface_is_rejected_when_ref_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
            .execute(
                "DELETE FROM lix_branch_descriptor WHERE id = '01930000-0000-7000-8000-000000000001'",
                &[],
            )
            .await
            .expect_err("descriptor delete through entity surface should fail");
        assert_branch_pair_delete_restricted(&error);

        assert_eq!(
            count_branch_descriptors(&main, "01930000-0000-7000-8000-000000000001").await,
            1
        );
        assert_eq!(
            count_branch_refs(&main, "01930000-0000-7000-8000-000000000001").await,
            1
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
                .await
                .expect("branch ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    branch_ref_delete_via_entity_surface_is_rejected_when_descriptor_exists,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;

        let error = main
            .execute(
                "DELETE FROM lix_branch_ref WHERE id = '01930000-0000-7000-8000-000000000001'",
                &[],
            )
            .await
            .expect_err("ref delete through entity surface should fail");
        assert_branch_pair_delete_restricted(&error);

        assert_eq!(
            count_branch_descriptors(&main, "01930000-0000-7000-8000-000000000001").await,
            1
        );
        assert_eq!(
            count_branch_refs(&main, "01930000-0000-7000-8000-000000000001").await,
            1
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
                .await
                .expect("branch ref head should still load"),
            Some(sim.initial_commit_id().to_string())
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(
    create_branch_can_start_from_explicit_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
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
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000003".to_string()),
                name: "From initial".to_string(),
                from_commit_id: Some(sim.initial_commit_id().to_string()),
            })
            .await
            .expect("branch should be created from explicit commit");
        assert_eq!(receipt.id, "01930000-0000-7000-8000-000000000003");
        assert_eq!(receipt.name, "From initial");
        assert!(!receipt.hidden);
        assert_eq!(receipt.commit_id, sim.initial_commit_id());
        assert_eq!(
            engine
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000003")
                .await
                .expect("branch head should load"),
            Some(sim.initial_commit_id().to_string())
        );

        let from_initial = main.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000003")
                .await
                .expect("explicit commit branch session should open"),
            &engine,
        );
        assert_key_value(&from_initial, "main-after-initial", None).await;

        drop(from_initial);
        drop(main);
        drop(engine);
    }
);

simulation_test!(
    create_branch_rejects_missing_explicit_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        let missing_commit_id = "ffffffff-ffff-4fff-bfff-ffffffffffff";
        let error = main
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000004".to_string()),
                name: "From missing".to_string(),
                from_commit_id: Some(missing_commit_id.to_string()),
            })
            .await
            .expect_err("creating a branch from a missing commit should fail");

        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("commit_id").and_then(JsonValue::as_str)),
            Some(missing_commit_id)
        );

        drop(main);
        drop(engine);
    }
);

simulation_test!(created_branch_sees_inherited_state, |sim| async move {
    let (_engine, _main, draft) = create_draft_after_shared_write(&sim).await;

    assert_key_value(
        &draft,
        "73686172-6564-8d62-8566-6f72652d6200",
        Some("\"shared\""),
    )
    .await;
});

simulation_test!(
    open_session_starts_on_seeded_repository_default_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );

        assert_eq!(
            session
                .active_branch_id()
                .await
                .expect("active branch should resolve"),
            sim.main_branch_id()
        );
    }
);

simulation_test!(
    later_main_changes_do_not_appear_in_created_branch,
    |sim| async move {
        let (_engine, main, draft) = create_draft_from_main(&sim).await;

        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('6d61696e-2d61-8674-8572-2d6272616e00', 'main')",
            &[],
        )
        .await
        .expect("main write should succeed");

        assert_key_value(
            &main,
            "6d61696e-2d61-8674-8572-2d6272616e00",
            Some("\"main\""),
        )
        .await;
        assert_key_value(&draft, "6d61696e-2d61-8674-8572-2d6272616e00", None).await;
    }
);

simulation_test!(
    later_created_branch_changes_do_not_appear_in_main,
    |sim| async move {
        let (_engine, main, draft) = create_draft_from_main(&sim).await;

        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('64726166-742d-8166-8465-722d62726100', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");
        assert_key_value(
            &draft,
            "64726166-742d-8166-8465-722d62726100",
            Some("\"draft\""),
        )
        .await;
        assert_key_value(&main, "64726166-742d-8166-8465-722d62726100", None).await;
    }
);

simulation_test!(switch_branch_updates_session_in_place, |sim| async move {
    let (engine, main, draft) = create_draft_from_main(&sim).await;
    draft
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('switch-draft-only', 'draft')",
            &[],
        )
        .await
        .expect("draft write should succeed");
    let main_clone = main.clone();

    let receipt = main
        .switch_branch(SwitchBranchOptions {
            branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("switch should succeed");

    assert_eq!(receipt.branch_id, "01930000-0000-7000-8000-000000000001");
    assert_key_value(&main, "switch-draft-only", Some("\"draft\"")).await;
    assert_key_value(&main_clone, "switch-draft-only", Some("\"draft\"")).await;

    drop(engine);
});

simulation_test!(cannot_delete_repository_default_branch, |sim| async move {
    let (_engine, _main, draft) = create_draft_from_main(&sim).await;

    let error = draft
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(sim.main_branch_id().to_string())],
        )
        .await
        .expect_err("repository default branch deletion should fail");

    assert!(
        error
            .message
            .contains("cannot delete repository default branch"),
        "unexpected error: {error:?}"
    );
});

simulation_test!(
    cached_write_templates_are_isolated_across_branch_switches,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;
        let main_snapshot = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id().to_string())
                .await
                .expect("open independent main session"),
            &engine,
        );
        let insert_sql = "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)";

        main.execute(
            insert_sql,
            &[
                Value::Text("template-main-only".to_string()),
                Value::Text("main".to_string()),
            ],
        )
        .await
        .expect("main write should warm the exact SQL template");

        main.switch_branch(SwitchBranchOptions {
            branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("switch should succeed");
        main.execute(
            insert_sql,
            &[
                Value::Text("template-draft-only".to_string()),
                Value::Text("draft".to_string()),
            ],
        )
        .await
        .expect("the same SQL should bind to the switched branch");

        assert_key_value(&main, "template-draft-only", Some("\"draft\"")).await;
        assert_key_value(&main_snapshot, "template-draft-only", None).await;
        assert_key_value(&main_snapshot, "template-main-only", Some("\"main\"")).await;
        assert_key_value(&main, "template-main-only", None).await;

        drop(engine);
    }
);

simulation_test!(
    switch_branch_is_session_local_and_does_not_advance_refs,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load");
        let draft_head_before = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load");
        let default_before = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("default session should open"),
            &engine,
        );
        assert_eq!(
            default_before
                .active_branch_id()
                .await
                .expect("default branch should resolve"),
            sim.main_branch_id(),
            "session setup should not move the repository default"
        );

        main.switch_branch(SwitchBranchOptions {
            branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("switch should succeed");

        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            main_head_before,
            "switching must not mutate the source session branch ref"
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
                .await
                .expect("draft head should load"),
            draft_head_before,
            "switching must not mutate the target branch ref"
        );
        let default_after = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("default session should open"),
            &engine,
        );
        assert_eq!(
            default_after
                .active_branch_id()
                .await
                .expect("default branch should resolve"),
            sim.main_branch_id(),
            "switching must not mutate the repository default"
        );
    }
);

simulation_test!(
    independently_opened_sessions_keep_independent_branch_selection,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('session-draft-only', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load");
        let draft_head_before = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load");

        let session_a = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let session_b = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("second session should open"),
            &engine,
        );
        assert_eq!(
            session_a
                .active_branch_id()
                .await
                .expect("session branch should resolve"),
            sim.main_branch_id()
        );

        let receipt = session_a
            .switch_branch(SwitchBranchOptions {
                branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("session switch should succeed");

        assert_eq!(receipt.branch_id, "01930000-0000-7000-8000-000000000001");
        assert_eq!(
            session_a
                .active_branch_id()
                .await
                .expect("switched session branch should resolve"),
            "01930000-0000-7000-8000-000000000001"
        );
        assert_eq!(
            session_b
                .active_branch_id()
                .await
                .expect("independent session should retain its branch"),
            sim.main_branch_id(),
            "independent sessions retain their own branch selection"
        );
        assert_key_value(&session_b, "session-draft-only", None).await;
        let session_c = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("new default session should open"),
            &engine,
        );
        assert_eq!(
            session_c
                .active_branch_id()
                .await
                .expect("repository default should resolve"),
            sim.main_branch_id()
        );
        assert_key_value(&session_c, "session-draft-only", None).await;
        assert_key_value(&main, "session-draft-only", None).await;
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            main_head_before,
            "session switching must not mutate the old branch ref"
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
                .await
                .expect("draft head should load"),
            draft_head_before,
            "session switching must not mutate the new branch ref"
        );
    }
);

simulation_test!(
    session_switch_does_not_persist_across_reopened_engine,
    |sim| async move {
        let (engine, _main, draft) = create_draft_from_main(&sim).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('session-reopen-draft', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .switch_branch(SwitchBranchOptions {
                branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("session switch should succeed");

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reopen from current snapshot");
        let reopened_session = sim.wrap_session(
            reopened_engine
                .open_session()
                .await
                .expect("reopened session should open"),
            &reopened_engine,
        );

        assert_eq!(
            reopened_session
                .active_branch_id()
                .await
                .expect("repository default should resolve after reopen"),
            sim.main_branch_id(),
            "session switching must not change the repository default"
        );
        assert_key_value(&reopened_session, "session-reopen-draft", None).await;
    }
);

simulation_test!(
    switch_branch_errors_when_target_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = main
            .switch_branch(SwitchBranchOptions {
                branch_id: "6d697373-696e-872d-8272-616e63680000".to_string(),
            })
            .await;
        let Err(error) = result else {
            panic!("missing branch ref should fail");
        };

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("branch_id")),
            Some(&JsonValue::String(
                "6d697373-696e-872d-8272-616e63680000".to_string()
            ))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("operation")),
            Some(&JsonValue::String("switch_branch".to_string()))
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
    merge_branch_resolves_existing_source_and_target_heads,
    |sim| async move {
        let (engine, main, _draft) = create_draft_from_main(&sim).await;
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge head resolution should succeed");

        assert_eq!(receipt.outcome, MergeBranchOutcome::AlreadyUpToDate);
        assert_eq!(receipt.change_stats, MergeChangeStats::default());
        assert_eq!(receipt.created_merge_commit_id, None);
        assert_eq!(receipt.target_branch_id, sim.main_branch_id());
        assert_eq!(
            receipt.source_branch_id,
            "01930000-0000-7000-8000-000000000001"
        );
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
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            Some(main_head_before)
        );
    }
);

simulation_test!(
    merge_branch_fast_forwards_when_target_is_merge_base,
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge preview should analyze fast-forward");
        assert_eq!(preview.outcome, MergeBranchOutcome::FastForward);
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
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load")
                .as_deref(),
            Some(target_head_before.as_str()),
            "preview should not advance the target ref"
        );

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge should fast-forward target");
        assert_eq!(receipt.outcome, MergeBranchOutcome::FastForward);
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
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load")
                .as_deref(),
            Some(source_head.as_str())
        );
        assert_key_value(&main, "draft-fast-forward", Some("\"draft\"")).await;

        let global = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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
    merge_branch_advances_target_with_two_parent_commit,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        main.create_checkpoint()
            .await
            .expect("target checkpoint should succeed");
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge should apply source change");
        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
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
            .load_branch_head_commit_id(sim.main_branch_id())
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
                .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
                .await
                .expect("draft head should load")
                .as_deref(),
            Some(source_head.as_str()),
            "merging into main must not move the source branch ref"
        );

        assert_key_value(&main, "draft-merge-source", Some("\"draft\"")).await;
        assert_key_value(&main, "main-merge-target", Some("\"main\"")).await;
        let working_diffs = main
            .execute(
                "SELECT entity_pk, diff_type \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_pk",
                &[],
            )
            .await
            .expect("post-merge working diffs should load");
        assert_eq!(working_diffs.len(), 2);
        assert_eq!(
            working_diffs.rows()[0].values(),
            &[
                Value::Json(
                    JsonValue::Array(vec![JsonValue::String("draft-merge-source".to_string())])
                        .into()
                ),
                Value::Text("added".to_string()),
            ],
            "the selected source delta must remain visible against the target checkpoint"
        );
        assert_eq!(
            working_diffs.rows()[1].values(),
            &[
                Value::Json(
                    JsonValue::Array(vec![JsonValue::String("main-merge-target".to_string())])
                        .into()
                ),
                Value::Text("added".to_string()),
            ],
            "the target delta must remain visible against the target checkpoint"
        );

        let global = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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
    merge_branch_rejects_selected_tracked_identity_that_conflicts_with_local_untracked,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-untracked-target-change', 'target')",
            &[],
        )
        .await
        .expect("main write should force a merge commit instead of fast-forward");
        main.execute(
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
             VALUES ('merge-selected-untracked-conflict', 'target-untracked', true)",
            &[],
        )
        .await
        .expect("target untracked row should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('merge-selected-untracked-conflict', 'source-tracked')",
                &[],
            )
            .await
            .expect("source tracked row should succeed");

        let target_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect_err("merge must reject a selected tracked/untracked identity collision");

        assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
        assert!(
            error.message.contains("untracked current row"),
            "unexpected merge error: {error:?}"
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("kind"))
                .and_then(JsonValue::as_str),
            Some("trackedUntrackedIdentityCollision")
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load")
                .as_deref(),
            Some(target_head_before.as_str()),
            "rejected merge must not publish a branch move"
        );
        assert_key_value(
            &main,
            "merge-selected-untracked-conflict",
            Some("\"target-untracked\""),
        )
        .await;
        assert_key_value(
            &draft,
            "merge-selected-untracked-conflict",
            Some("\"source-tracked\""),
        )
        .await;
    }
);

simulation_test!(
    merge_branch_does_not_republish_global_checkpoint_entity,
    |sim| async move {
        let (_engine, main, draft) = create_draft_from_main(&sim).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-merge-target', 'main')",
            &[],
        )
        .await
        .expect("main write should diverge the target");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-merge-source', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");
        let checkpoint = draft
            .create_checkpoint()
            .await
            .expect("draft checkpoint should succeed");
        assert_eq!(
            main.execute(
                "SELECT commit_id FROM lix_checkpoint WHERE commit_id = $1",
                &[Value::Text(checkpoint.commit_id.clone())],
            )
            .await
            .expect("global checkpoint should be visible from main")
            .rows()[0]
                .values(),
            &[Value::Text(checkpoint.commit_id.clone())],
            "a checkpoint is a global entity inherited by every branch"
        );

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("checkpointed source should merge");
        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
        assert_eq!(
            receipt.change_stats,
            MergeChangeStats {
                total: 1,
                added: 1,
                modified: 0,
                removed: 0,
            },
            "checkpoint metadata must not count as a merged user change"
        );

        let checkpoints = main
            .execute(
                "SELECT commit_id FROM lix_checkpoint WHERE commit_id = $1",
                &[Value::Text(checkpoint.commit_id.clone())],
            )
            .await
            .expect("global checkpoint should remain queryable");
        assert_eq!(
            checkpoints.len(),
            1,
            "merge must not duplicate the checkpoint entity"
        );
        assert_eq!(
            checkpoints.rows()[0].values(),
            &[Value::Text(checkpoint.commit_id)]
        );
        assert_key_value(&main, "checkpoint-merge-source", Some("\"draft\"")).await;
    }
);

simulation_test!(
    checkpoint_preserves_branch_fork_as_merge_ancestry,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES \
             ('checkpoint-fork-source', 'shared-source'), \
             ('checkpoint-fork-target', 'shared-target')",
            &[],
        )
        .await
        .expect("shared rows should be inserted");
        let fork_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let checkpoint = main
            .create_checkpoint()
            .await
            .expect("target checkpoint should succeed");
        let branch = main
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000001".to_string()),
                name: "Recovered source".to_string(),
                from_commit_id: Some(fork_commit_id.clone()),
            })
            .await
            .expect("historical recovered head should remain branchable while pending");
        assert_eq!(branch.commit_id, fork_commit_id);
        let draft = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000001")
                .await
                .expect("recovered source session should open"),
            &engine,
        );
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'source' \
                 WHERE key = 'checkpoint-fork-source'",
                &[],
            )
            .await
            .expect("source edit should succeed");
        let source_commit_id = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("source head should load")
            .expect("source head should exist");
        main.execute(
            "UPDATE lix_key_value SET value = 'target' \
             WHERE key = 'checkpoint-fork-target'",
            &[],
        )
        .await
        .expect("target edit should succeed");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("disjoint changes should merge without conflicts");
        assert_eq!(preview.base_commit_id, checkpoint.commit_id);
        assert_eq!(preview.outcome, MergeBranchOutcome::MergeCommitted);
        assert_eq!(preview.conflicts.len(), 0);
        assert_eq!(
            preview.change_stats,
            MergeChangeStats {
                total: 1,
                added: 0,
                modified: 1,
                removed: 0,
            }
        );

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("disjoint changes should merge");
        assert_eq!(receipt.base_commit_id, checkpoint.commit_id);
        assert_key_value(&main, "checkpoint-fork-source", Some("\"source\"")).await;
        assert_key_value(&main, "checkpoint-fork-target", Some("\"target\"")).await;

        let global = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .await
                .expect("global session should open"),
            &engine,
        );
        assert_eq!(
            commit_parent_edges(&global, &checkpoint.commit_id).await,
            vec![(sim.initial_commit_id().to_string(), 0)],
            "checkpoint compaction must not retain the recovered interval as permanent ancestry",
        );
        assert_eq!(
            commit_parent_edges(&global, &source_commit_id).await,
            vec![(fork_commit_id, 0), (checkpoint.commit_id, 1)],
            "the first source commit should consume serving context into canonical graph parents",
        );
    }
);

simulation_test!(
    checkpoint_branch_bridge_preserves_true_conflict,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-bridge-conflict', 'base')",
            &[],
        )
        .await
        .expect("shared conflict row should insert");
        let recovered_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("recovered head should load")
            .expect("recovered head should exist");
        let checkpoint = main
            .create_checkpoint()
            .await
            .expect("checkpoint should succeed")
            .commit_id;
        main.create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000002".to_string()),
            name: "Recovered conflict source".to_string(),
            from_commit_id: Some(recovered_head.clone()),
        })
        .await
        .expect("recovered conflict source should remain branchable");
        let source = sim.wrap_session(
            engine
                .open_session_at("01930000-0000-7000-8000-000000000002")
                .await
                .expect("conflict source session should open"),
            &engine,
        );
        source
            .execute(
                "UPDATE lix_key_value SET value = 'source' WHERE key = 'checkpoint-bridge-conflict'",
                &[],
            )
            .await
            .expect("source conflict edit should succeed");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000002")
            .await
            .expect("source conflict head should load")
            .expect("source conflict head should exist");
        main.execute(
            "UPDATE lix_key_value SET value = 'target' WHERE key = 'checkpoint-bridge-conflict'",
            &[],
        )
        .await
        .expect("target conflict edit should succeed");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000002".to_string(),
            })
            .await
            .expect("true conflict preview should succeed");
        assert_eq!(preview.base_commit_id, checkpoint);
        assert_eq!(preview.conflicts.len(), 1);
        assert_eq!(
            commit_parent_edges(&main, &source_head).await,
            vec![(recovered_head, 0), (checkpoint, 1)]
        );
        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000002".to_string(),
            })
            .await
            .expect_err("same-identity changes must still conflict");
        assert_merge_conflict_error(&error);
        assert_key_value(&main, "checkpoint-bridge-conflict", Some("\"target\"")).await;
    }
);

simulation_test!(
    merge_branch_selects_source_change_without_minting_equivalent_copy,
    |sim| async move {
        let (engine, main, draft) = create_draft_from_main(&sim).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-select-target', 'target')",
            &[],
        )
        .await
        .expect("main write should succeed");
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-select-change', 'source')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge should apply source change");
        assert!(
            receipt.created_merge_commit_id.is_some(),
            "non-empty merge should create a merge commit"
        );

        let global = sim.wrap_session(
            engine
                .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .await
                .expect("global session should open"),
            &engine,
        );
        let equivalent_change_count = select_single_integer(
            &global,
            "SELECT count(*) \
	     FROM lix_change \
	     WHERE schema_key = 'lix_key_value' \
	       AND entity_pk = CAST('[\"merge-select-change\"]' AS JSONB) \
	       AND snapshot_content = CAST('{\"key\":\"merge-select-change\",\"value\":\"source\"}' AS JSONB)",
        )
        .await;
        assert_eq!(
            equivalent_change_count, 1,
            "merge must not append a second canonical change with identical effect"
        );

        let history = main
            .execute(
                "SELECT value \
	             FROM lix_key_value_history() \
	               WHERE key = 'merge-select-change' \
	             ORDER BY lixcol_depth",
                &[],
            )
            .await
            .expect("history query should succeed");
        assert_eq!(
            history.len(),
            1,
            "history should show the selected canonical change once, not once from the merge commit and once from the source parent"
        );
    }
);

simulation_test!(
    merge_branch_selects_schema_registration_before_schema_rows,
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
                 CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"merge_task_item\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB)\
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

        main.merge_branch(MergeBranchOptions {
            source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("merge should select schema registration before rows that use it");

        let reopened_main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
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
    merge_branch_errors_on_divergent_same_entity_change,
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect_err("divergent same-entity changes should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target branch ref"
        );
        assert_key_value(&main, "merge-conflict", Some("\"main\"")).await;
    }
);

simulation_test!(
    merge_branch_fast_forwards_source_delete_when_target_unchanged,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&draft, "73686172-6564-8d62-8566-6f72652d6200").await;
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge should apply source delete");

        assert_eq!(receipt.outcome, MergeBranchOutcome::FastForward);
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
        assert_key_value(&main, "73686172-6564-8d62-8566-6f72652d6200", None).await;
    }
);

simulation_test!(
    merge_branch_records_empty_merge_when_both_sides_delete,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&main, "73686172-6564-8d62-8566-6f72652d6200").await;
        delete_key_value(&draft, "73686172-6564-8d62-8566-6f72652d6200").await;
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("convergent delete merge should succeed");

        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
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
        assert_key_value(&main, "73686172-6564-8d62-8566-6f72652d6200", None).await;
    }
);

simulation_test!(
    merge_branch_conflicts_when_target_deletes_source_modifies,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        delete_key_value(&main, "73686172-6564-8d62-8566-6f72652d6200").await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'draft' WHERE key = '73686172-6564-8d62-8566-6f72652d6200'",
                &[],
            )
            .await
            .expect("draft update should succeed");
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect_err("delete/modify should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target branch ref"
        );
        assert_key_value(&main, "73686172-6564-8d62-8566-6f72652d6200", None).await;
    }
);

simulation_test!(
    merge_branch_conflicts_when_target_modifies_source_deletes,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        main.execute(
            "UPDATE lix_key_value SET value = 'main' WHERE key = '73686172-6564-8d62-8566-6f72652d6200'",
            &[],
        )
        .await
        .expect("main update should succeed");
        delete_key_value(&draft, "73686172-6564-8d62-8566-6f72652d6200").await;
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect_err("modify/delete should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target branch ref"
        );
        assert_key_value(
            &main,
            "73686172-6564-8d62-8566-6f72652d6200",
            Some("\"main\""),
        )
        .await;
    }
);

simulation_test!(
    merge_branch_records_empty_merge_for_same_payload_convergence,
    |sim| async move {
        let (engine, main, draft) = create_draft_after_shared_write(&sim).await;

        main.execute(
            "UPDATE lix_key_value SET value = 'same' WHERE key = '73686172-6564-8d62-8566-6f72652d6200'",
            &[],
        )
        .await
        .expect("main update should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'same' WHERE key = '73686172-6564-8d62-8566-6f72652d6200'",
                &[],
            )
            .await
            .expect("draft update should succeed");
        let main_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("convergent update merge should succeed");

        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
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
        assert_key_value(
            &main,
            "73686172-6564-8d62-8566-6f72652d6200",
            Some("\"same\""),
        )
        .await;
    }
);

simulation_test!(
    merge_branch_conflicts_on_independent_add_same_identity_different_payload,
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect_err("independent adds with different payloads should conflict");
        assert_merge_conflict_error(&error);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("main head should load"),
            Some(main_head_before),
            "failed merge should not advance the target branch ref"
        );
        assert_key_value(&main, "merge-independent-add", Some("\"main\"")).await;
    }
);

simulation_test!(
    merge_branch_records_empty_merge_for_same_identity_same_payload_add,
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
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
        let source_head = engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000001")
            .await
            .expect("draft head should load")
            .expect("draft head should exist");

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("convergent independent add merge should succeed");

        assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
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
    merge_branch_errors_when_source_branch_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "6d697373-696e-872d-8272-616e63680000".to_string(),
            })
            .await
            .expect_err("missing source ref should fail");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("branch_id")),
            Some(&JsonValue::String(
                "6d697373-696e-872d-8272-616e63680000".to_string()
            ))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("operation")),
            Some(&JsonValue::String("merge_branch".to_string()))
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

simulation_test!(merge_branch_rejects_self_merge, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    let error = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: sim.main_branch_id().to_string(),
        })
        .await
        .expect_err("self-merge should fail");

    assert_eq!(error.code, LixError::CODE_INVALID_MERGE);
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("operation")),
        Some(&JsonValue::String("merge_branch".to_string()))
    );
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("target_branch_id")),
        Some(&JsonValue::String(sim.main_branch_id().to_string()))
    );
    assert_eq!(
        error
            .details
            .as_ref()
            .and_then(|details| details.get("source_branch_id")),
        Some(&JsonValue::String(sim.main_branch_id().to_string()))
    );
});

async fn delete_key_value(session: &support::simulation_test::engine::SimSession, key: &str) {
    session
        .execute(
            &format!("DELETE FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("key-value delete should succeed");
}

async fn create_draft_after_shared_write(
    sim: &support::simulation_test::engine::Simulation,
) -> (
    Engine,
    support::simulation_test::engine::SimSession,
    support::simulation_test::engine::SimSession,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session_at(sim.main_branch_id())
            .await
            .expect("main session should open"),
        &engine,
    );
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('73686172-6564-8d62-8566-6f72652d6200', 'shared')",
        &[],
    )
    .await
    .expect("source write should succeed");

    let draft = create_draft(&engine, &main).await;
    (engine, main, draft)
}

async fn create_draft_from_main(
    sim: &support::simulation_test::engine::Simulation,
) -> (
    Engine,
    support::simulation_test::engine::SimSession,
    support::simulation_test::engine::SimSession,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session_at(sim.main_branch_id())
            .await
            .expect("main session should open"),
        &engine,
    );
    let draft = create_draft(&engine, &main).await;
    (engine, main, draft)
}

async fn create_draft(
    engine: &Engine,
    main: &support::simulation_test::engine::SimSession,
) -> support::simulation_test::engine::SimSession {
    let receipt = main
        .create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000001".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch should be created");
    assert_eq!(receipt.id, "01930000-0000-7000-8000-000000000001");
    let branch_row = main
        .execute(
            "SELECT id, name, hidden, commit_id FROM lix_branch WHERE id = '01930000-0000-7000-8000-000000000001'",
            &[],
        )
        .await
        .expect("created branch should be queryable through lix_branch");
    assert_eq!(branch_row.len(), 1);
    assert_eq!(
        branch_row.rows()[0].values(),
        &[
            Value::Text(receipt.id.clone()),
            Value::Text(receipt.name.clone()),
            Value::Boolean(receipt.hidden),
            Value::Text(receipt.commit_id.clone()),
        ],
        "create_branch should return the same public shape as lix_branch"
    );
    main.wrap_session(
        engine
            .open_session_at(receipt.id)
            .await
            .expect("draft session should open"),
        engine,
    )
}

async fn assert_key_value(
    session: &support::simulation_test::engine::SimSession,
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
            assert_eq!(
                rows.rows()[0].values(),
                &[Value::Json(expected_json.into())]
            );
        }
        None => assert_eq!(rows.len(), 0),
    }
}

async fn assert_branch_descriptor(
    session: &support::simulation_test::engine::SimSession,
    branch_id: &str,
    expected_name: &str,
) {
    let result = session
        .execute(
            &format!("SELECT id, name FROM lix_branch WHERE id = '{branch_id}'"),
            &[],
        )
        .await
        .expect("branch query should succeed");
    let rows = result;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text(branch_id.to_string()),
            Value::Text(expected_name.to_string()),
        ]
    );
}

async fn count_branch_descriptors(
    session: &support::simulation_test::engine::SimSession,
    branch_id: &str,
) -> i64 {
    select_single_integer(
        session,
        &format!("SELECT COUNT(*) FROM lix_branch_descriptor WHERE id = '{branch_id}'"),
    )
    .await
}

async fn count_branch_refs(
    session: &support::simulation_test::engine::SimSession,
    branch_id: &str,
) -> i64 {
    select_single_integer(
        session,
        &format!("SELECT COUNT(*) FROM lix_branch_ref WHERE id = '{branch_id}'"),
    )
    .await
}

fn assert_branch_pair_delete_restricted(error: &LixError) {
    assert_eq!(error.code, LixError::CODE_READ_ONLY);
    assert!(
        error.to_string().contains("lix_branch"),
        "error should explain the branch pair restriction: {error:?}"
    );
    assert!(
        error
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("lix_branch")),
        "error should guide callers to the lix_branch surface: {error:?}"
    );
}

fn assert_merge_conflict_error(error: &LixError) {
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
            .get("entityPk")
            .and_then(JsonValue::as_array)
            .is_some(),
        "conflict should include entityPk: {conflict:?}"
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
    session: &support::simulation_test::engine::SimSession,
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
    session: &support::simulation_test::engine::SimSession,
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
    session: &support::simulation_test::engine::SimSession,
    merge_commit_id: &str,
    target_head_before: &str,
    source_head: &str,
) {
    let active_branch_id = session
        .active_branch_id()
        .await
        .expect("active branch should load");
    assert_eq!(
        engine
            .load_branch_head_commit_id(&active_branch_id)
            .await
            .expect("target branch head should load")
            .as_deref(),
        Some(merge_commit_id),
        "empty merge should advance the target branch ref"
    );

    let global = session.wrap_session(
        engine
            .open_session_at("ffffffff-ffff-7fff-bfff-ffffffffffff")
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

// ---------------------------------------------------------------------------
// Regression: root-backed generations must not lose the working-diff baseline.
// ---------------------------------------------------------------------------

simulation_test!(
    working_diff_first_branch_edit_of_existing_row_is_modified,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-baseline', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;

        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-baseline'",
                &[],
            )
            .await
            .expect("first branch edit should succeed");

        let rows = draft
            .execute(
                "SELECT diff_type, before_change_id, after_change_id \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = CAST('[\"branch-baseline\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("working diff should load");
        assert_eq!(
            rows.len(),
            1,
            "the edited row must appear in the working diff"
        );
        let values = rows.rows()[0].values().to_vec();
        assert!(
            matches!(&values[1], Value::Text(_)),
            "the first branch-local edit of a checkpointed row must carry a before image, got {values:?}"
        );
        assert_eq!(
            values[0],
            Value::Text("modified".to_string()),
            "the first branch-local edit of a checkpointed row must classify as modified, got {values:?}"
        );
    }
);

simulation_test!(
    revert_of_first_branch_edit_restores_the_checkpoint_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-revert', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-revert'",
                &[],
            )
            .await
            .expect("first branch edit should succeed");

        draft
            .execute(
                "INSERT INTO lix_revert (diff_id) \
                 SELECT diff_id FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = CAST('[\"branch-revert\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("revert should succeed");

        assert_key_value(&draft, "branch-revert", Some("\"before\"")).await;
    }
);

simulation_test!(
    working_diff_first_edit_after_merge_of_existing_row_is_modified,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-baseline', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-unrelated', 'draft')",
                &[],
            )
            .await
            .expect("draft write should succeed");

        main.create_checkpoint()
            .await
            .expect("target checkpoint should succeed");
        main.merge_branch(MergeBranchOptions {
            source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("merge should succeed");

        main.execute(
            "UPDATE lix_key_value SET value = 'after' WHERE key = 'merge-baseline'",
            &[],
        )
        .await
        .expect("first post-merge edit should succeed");

        let rows = main
            .execute(
                "SELECT diff_type, before_change_id \
                 FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = CAST('[\"merge-baseline\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("working diff should load");
        assert_eq!(
            rows.len(),
            1,
            "the edited row must appear in the working diff"
        );
        let values = rows.rows()[0].values().to_vec();
        assert!(
            matches!(&values[1], Value::Text(_)),
            "the first post-merge edit of a checkpointed row must carry a before image, got {values:?}"
        );
        assert_eq!(
            values[0],
            Value::Text("modified".to_string()),
            "the first post-merge edit of a checkpointed row must classify as modified, got {values:?}"
        );
    }
);

simulation_test!(
    working_diff_first_branch_delete_of_existing_row_is_removed,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-delete', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        delete_key_value(&draft, "branch-delete").await;

        let narrow = draft
            .execute(
                "SELECT diff_type, before_change_id FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = CAST('[\"branch-delete\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("narrow working diff should load");
        assert_eq!(
            narrow.len(),
            1,
            "the deleted row must appear in the narrow working diff"
        );
        assert_eq!(
            narrow.rows()[0].values()[0],
            Value::Text("removed".to_string()),
            "deleting a checkpointed row on a fresh branch must classify as removed, got {:?}",
            narrow.rows()[0].values()
        );

        let broad = draft
            .execute(
                "SELECT entity_pk, diff_type FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
                &[],
            )
            .await
            .expect("broad working diff should load");
        assert_eq!(
            broad.len(),
            1,
            "the deleted row must also appear in the unfiltered working diff, got {:?}",
            broad
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>()
        );
    }
);

simulation_test!(
    working_diff_branch_edit_is_modified_for_broad_and_repeated_edits,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-broad-a', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-broad-b', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('branch-broad-new', 'new')",
                &[],
            )
            .await
            .expect("unrelated branch insert should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-broad-a'",
                &[],
            )
            .await
            .expect("first edit should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after-2' WHERE key = 'branch-broad-a'",
                &[],
            )
            .await
            .expect("second edit of the same row should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-broad-b'",
                &[],
            )
            .await
            .expect("edit of a second pre-existing row should succeed");

        let rows = draft
            .execute(
                "SELECT entity_pk, diff_type FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
                &[],
            )
            .await
            .expect("broad working diff should load");
        let actual = rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                vec![
                    Value::Json(
                        JsonValue::Array(vec![JsonValue::String("branch-broad-a".to_string())])
                            .into()
                    ),
                    Value::Text("modified".to_string()),
                ],
                vec![
                    Value::Json(
                        JsonValue::Array(vec![JsonValue::String("branch-broad-b".to_string())])
                            .into()
                    ),
                    Value::Text("modified".to_string()),
                ],
                vec![
                    Value::Json(
                        JsonValue::Array(vec![JsonValue::String("branch-broad-new".to_string())])
                            .into()
                    ),
                    Value::Text("added".to_string()),
                ],
            ],
            "every branch-local edit of a checkpointed row must classify as modified"
        );
    }
);

simulation_test!(
    working_diff_restoring_the_checkpoint_payload_on_a_branch_is_net_empty,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-restore', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-restore'",
                &[],
            )
            .await
            .expect("first branch edit should succeed");
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'before' WHERE key = 'branch-restore'",
                &[],
            )
            .await
            .expect("restoring the checkpoint payload should succeed");

        let rows = draft
            .execute(
                "SELECT entity_pk, diff_type FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' ORDER BY entity_pk",
                &[],
            )
            .await
            .expect("working diff should load");
        // The branch-local baseline stores only the before image's change id,
        // so this net-empty answer is exactly the case that forces the reader
        // to hydrate the referenced change record. A regression to change-id
        // equality alone would report `modified` here.
        assert_eq!(
            rows.len(),
            0,
            "restoring the checkpoint payload must be net empty, got {:?}",
            rows.rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>()
        );
    }
);

simulation_test!(
    working_diff_first_edit_after_switch_branch_of_existing_row_is_modified,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('switch-baseline', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");
        let draft = create_draft(&engine, &main).await;
        drop(draft);

        main.switch_branch(SwitchBranchOptions {
            branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
        })
        .await
        .expect("switch should succeed");

        main.execute(
            "UPDATE lix_key_value SET value = 'after' WHERE key = 'switch-baseline'",
            &[],
        )
        .await
        .expect("first edit after switch should succeed");

        let rows = main
            .execute(
                "SELECT diff_type, before_change_id FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_pk = CAST('[\"switch-baseline\"]' AS JSONB)",
                &[],
            )
            .await
            .expect("working diff should load");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.rows()[0].values()[0],
            Value::Text("modified".to_string()),
            "the first edit after a branch switch must classify as modified, got {:?}",
            rows.rows()[0].values()
        );
    }
);

simulation_test!(
    checkpoint_after_first_branch_edit_keeps_the_edited_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-checkpoint', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'branch-checkpoint'",
                &[],
            )
            .await
            .expect("first branch edit should succeed");
        draft
            .create_checkpoint()
            .await
            .expect("branch checkpoint should succeed");

        assert_key_value(&draft, "branch-checkpoint", Some("\"after\"")).await;
        let rows = draft
            .execute(
                "SELECT entity_pk FROM lix_working_diff WHERE schema_key = 'lix_key_value'",
                &[],
            )
            .await
            .expect("working diff should load");
        assert_eq!(
            rows.len(),
            0,
            "a fresh checkpoint has an empty working diff"
        );
    }
);

simulation_test!(
    merging_a_first_branch_edit_reports_modified_and_lands_the_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session_at(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('merge-first-edit', 'before')",
            &[],
        )
        .await
        .expect("seed write should succeed");
        main.create_checkpoint()
            .await
            .expect("checkpoint should succeed");

        let draft = create_draft(&engine, &main).await;
        draft
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'merge-first-edit'",
                &[],
            )
            .await
            .expect("first branch edit should succeed");

        let preview = main
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("preview should succeed");
        assert_eq!(
            preview.change_stats,
            MergeChangeStats {
                total: 1,
                added: 0,
                modified: 1,
                removed: 0,
            },
            "merge preview must see the branch-local edit as a modification"
        );

        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "01930000-0000-7000-8000-000000000001".to_string(),
            })
            .await
            .expect("merge should succeed");
        assert_eq!(
            receipt.change_stats,
            MergeChangeStats {
                total: 1,
                added: 0,
                modified: 1,
                removed: 0,
            },
            "merge must record the branch-local edit as a modification"
        );
        assert_key_value(&main, "merge-first-edit", Some("\"after\"")).await;
    }
);
