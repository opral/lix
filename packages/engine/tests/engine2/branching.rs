use crate::simulation_test2;
use lix_engine::engine2::{
    CreateVersionOptions, Engine, ExecuteResult, MergeVersionOptions, SwitchVersionOptions,
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
    switch_version_is_ephemeral_and_does_not_advance_refs,
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
    }
);

simulation_test2!(
    switch_version_errors_when_target_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");

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

        assert_eq!(receipt.merged_changes, 0);
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
        assert_eq!(receipt.merged_changes, 1);

        let target_head_after = engine
            .load_version_head_commit_id(sim.main_version_id())
            .await
            .expect("main head should load")
            .expect("main head should exist");
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

        let global = sim
            .open_global_session(&engine)
            .await
            .expect("global session should open");
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
    merge_version_errors_when_source_version_ref_is_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim
            .open_main_session(&engine)
            .await
            .expect("main session should open");

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

async fn create_draft_after_shared_write(
    sim: &crate::support::simulation_test::engine2::Engine2Simulation,
) -> (
    Engine,
    crate::support::simulation_test::engine2::SimSession,
    crate::support::simulation_test::engine2::SimSession,
) {
    let engine = sim.boot_engine().await;
    let main = sim
        .open_main_session(&engine)
        .await
        .expect("main session should open");
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('shared-before-branch', 'shared')",
        &[],
    )
    .await
    .expect("source write should succeed");

    let draft = create_draft(&engine, sim, &main).await;
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
    let main = sim
        .open_main_session(&engine)
        .await
        .expect("main session should open");
    let draft = create_draft(&engine, sim, &main).await;
    (engine, main, draft)
}

async fn create_draft(
    engine: &Engine,
    sim: &crate::support::simulation_test::engine2::Engine2Simulation,
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
    sim.open_session(engine, receipt.version_id)
        .await
        .expect("draft session should open")
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
